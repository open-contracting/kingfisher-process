import logging

from django.conf import settings
from django.db import connection, transaction
from django.db.models.functions import Now
from django.http.response import Http404
from django.shortcuts import get_object_or_404
from drf_spectacular.utils import extend_schema
from rest_framework import serializers, status, viewsets
from rest_framework.decorators import action
from rest_framework.response import Response

from process.models import Collection, CollectionNote
from process.processors.loader import create_collections
from process.util import create_note, get_publisher

logger = logging.getLogger(__name__)


# https://docs.djangoproject.com/en/4.2/topics/db/sql/#executing-custom-sql-directly
def dictfetchone(cursor):
    if row := cursor.fetchone():
        columns = [col[0] for col in cursor.description]
        return dict(zip(columns, row))
    return {}


class TreeSerializer(serializers.ModelSerializer):
    class Meta:
        model = Collection
        fields = "__all__"


class CreateCollectionSerializer(serializers.Serializer):
    # Identification
    source_id = serializers.CharField(
        help_text="The source from which the files were retrieved (the name of the spider if sourced from Scrapy)"
    )
    data_version = serializers.CharField(
        help_text="The time at which the files were retrieved in 'YYYY-MM-DD HH:MM:SS' format"
    )
    sample = serializers.BooleanField(help_text="Whether the files represent a sample from the source", required=False)

    # Steps
    upgrade = serializers.BooleanField(
        help_text="Whether to upgrade the collection to the latest OCDS version", required=False
    )
    compile = serializers.BooleanField(
        help_text="Whether to create compiled releases from the collection", required=False
    )
    check = serializers.BooleanField(help_text="Whether to run structural checks on the collection", required=False)

    # Other
    job = serializers.CharField(help_text="The Scrapyd job ID of the Kingfisher Collect crawl", required=False)
    note = serializers.CharField(help_text="A note to add to the collection", required=False)


class CloseCollectionSerializer(serializers.Serializer):
    reason = serializers.CharField(help_text="The reason why the spider was closed", required=False)
    stats = serializers.DictField(help_text="The crawl statistics", required=False)


class CollectionViewSet(viewsets.ViewSet):
    lookup_value_converter = "int"

    @extend_schema(
        request=CreateCollectionSerializer,
        responses={
            201: {
                "type": "object",
                "properties": {
                    "collection_id": {"type": "integer"},
                    "upgraded_collection_id": {"type": "integer"},
                    "compiled_collection_id": {"type": "integer"},
                },
            },
        },
    )
    def create(self, request):
        """
        Create an original collection and any derived collections.
        """
        serializer = CreateCollectionSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)

        if not settings.ENABLE_CHECKER and serializer.data.get("check"):
            logger.error("Checker is disabled. Set the ENABLE_CHECKER environment variable to enable.")

        collection, upgraded_collection, compiled_collection = create_collections(
            # Identification
            serializer.data.get("source_id"),
            serializer.data.get("data_version"),
            sample=serializer.data.get("sample", False),
            # Steps
            upgrade=serializer.data.get("upgrade", False),
            compile=serializer.data.get("compile", False),
            check=serializer.data.get("check", False),
            # Other
            scrapyd_job=serializer.data.get("job", ""),
            note=serializer.data.get("note"),
        )

        result = {"collection_id": collection.pk}
        if upgraded_collection:
            result["upgraded_collection_id"] = upgraded_collection.pk
        if compiled_collection:
            result["compiled_collection_id"] = compiled_collection.pk

        return Response(result)

    @extend_schema(request=CloseCollectionSerializer, responses={202: None})
    @action(detail=True, methods=["post"])
    def close(self, request, pk=None):
        """
        Publish a message to RabbitMQ to close a root collection and its derived collections, if any.
        """
        serializer = CloseCollectionSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)

        with transaction.atomic():
            try:
                collection = Collection.objects.select_for_update().get(pk=pk)
            except Collection.DoesNotExist:
                raise Http404

            upgraded_collection = collection.get_upgraded_collection()

            expected_files_count = serializer.data.get("stats", {}).get("kingfisher_process_expected_files_count", 0)

            collection.expected_files_count = expected_files_count
            collection.store_end_at = Now()
            collection.save(update_fields=["expected_files_count", "store_end_at"])
            if upgraded_collection:
                upgraded_collection.expected_files_count = expected_files_count
                upgraded_collection.store_end_at = Now()
                upgraded_collection.save(update_fields=["expected_files_count", "store_end_at"])

            if serializer.data.get("reason"):
                note = f"Spider close reason: {serializer.data['reason']}"

                create_note(collection, CollectionNote.Level.INFO, note)
                if upgraded_collection:
                    create_note(upgraded_collection, CollectionNote.Level.INFO, note)

            if serializer.data.get("stats"):
                stats = serializer.data["stats"]

                create_note(collection, CollectionNote.Level.INFO, "Spider stats", data=stats)
                if upgraded_collection:
                    create_note(upgraded_collection, CollectionNote.Level.INFO, "Spider stats", data=stats)

        with get_publisher() as client:
            client.publish({"collection_id": collection.pk}, routing_key="collection_closed")
            if upgraded_collection:
                client.publish({"collection_id": upgraded_collection.pk}, routing_key="collection_closed")
            if compiled_collection := collection.get_compiled_collection():
                client.publish({"collection_id": compiled_collection.pk}, routing_key="collection_closed")

        return Response(status=status.HTTP_202_ACCEPTED)

    @extend_schema(responses={202: None})
    def destroy(self, request, pk=None):
        """
        Publish a message to RabbitMQ to wipe the dataset.
        """
        with get_publisher() as client:
            client.publish({"collection_id": pk}, routing_key="wiper")

        return Response(status=status.HTTP_202_ACCEPTED)

    @extend_schema(
        responses={
            200: {
                "type": "object",
                "properties": {
                    "ocid_prefix": {"type": "string"},
                    "published_from": {"type": "string"},
                    "published_to": {"type": "string"},
                    "license": {"type": "string"},
                    "publication_policy": {"type": "string"},
                },
            }
        }
    )
    @action(detail=True)
    def metadata(self, request, pk=None):
        """
        Return the compiled collection's metadata.
        """
        compiled_collection = get_object_or_404(Collection, id=pk)
        root_collection = compiled_collection.get_root_parent()

        if compiled_collection.transform_type != Collection.Transform.COMPILE_RELEASES:
            return Response("The collection id must be a compiled collection", status=status.HTTP_400_BAD_REQUEST)

        metadata = {}

        with connection.cursor() as cursor:
            cursor.execute(
                """\
                SELECT
                    LEFT(MAX(ocid), 11) as ocid_prefix,
                    MIN(data ->> 'date') as published_from,
                    MAX(data ->> 'date') as published_to
                FROM
                    compiled_release
                    JOIN data ON compiled_release.data_id = data.id
                WHERE
                    collection_id = %(collection_id)s
                    AND data ? 'date'
                    AND data ->> 'date' IS NOT NULL
                    AND data ->> 'date' <> ''
                """,
                {"collection_id": pk},
            )
            metadata.update(dictfetchone(cursor))

            cursor.execute(
                """\
                SELECT
                    data ->> 'license' AS license,
                    data ->> 'publicationPolicy' AS publication_policy
                FROM
                    package_data
                    LEFT JOIN record ON package_data.id = record.package_data_id
                        AND record.collection_id = %(collection_id)s
                    LEFT JOIN release ON package_data.id = release.package_data_id
                        AND release.collection_id = %(collection_id)s
                LIMIT 1
            """,
                {"collection_id": root_collection.id},
            )
            metadata.update(dictfetchone(cursor))

        return Response(metadata)

    @extend_schema(responses=TreeSerializer(many=True))
    @action(detail=True)
    def tree(self, request, pk=None):
        """
        Return the original collection and its derived collections, if any.
        """
        result = Collection.objects.raw(
            """\
            WITH RECURSIVE tree(id, parent, root, deep) AS (
                SELECT c.id, c.transform_from_collection_id AS parent, id AS root, 1 AS deep
                FROM collection c
                WHERE c.transform_from_collection_id IS NULL
            UNION ALL
                SELECT c.id, c.transform_from_collection_id, t.root, t.deep + 1
                FROM collection c, tree t
                WHERE c.transform_from_collection_id = t.id
            )
            SELECT c.*
            FROM tree t
            JOIN collection c on (t.id = c.id)
            WHERE t.root = %s
            ORDER BY deep ASC;
            """,
            [pk],
        )

        if not result:
            raise Http404

        serializer = TreeSerializer(result, many=True)
        return Response(serializer.data)
