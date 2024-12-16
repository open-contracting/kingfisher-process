import datetime
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
        return dict(zip(columns, row, strict=True))
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
    job = serializers.CharField(help_text="The Scrapyd job ID of the Scrapy crawl", required=False)
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
        """Create an original collection and any derived collections."""
        serializer = CreateCollectionSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)

        if not settings.ENABLE_CHECKER and serializer.data.get("check"):
            logger.error("Checker is disabled. Set the ENABLE_CHECKER environment variable to enable.")

        collection, upgraded_collection, compiled_collection = create_collections(
            # Identification
            serializer.data["source_id"],
            serializer.data["data_version"],
            sample=serializer.data.get("sample", False),
            # Steps
            upgrade=serializer.data.get("upgrade", False),
            compile=serializer.data.get("compile", False),
            check=serializer.data.get("check", False),
            # Other
            scrapyd_job=serializer.data.get("job", ""),
            note=serializer.data.get("note", ""),
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
        """Publish a message to RabbitMQ to close a root collection and its derived collections, if any."""
        serializer = CloseCollectionSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)

        with transaction.atomic():
            collection = get_object_or_404(Collection, pk=pk)
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

            if serializer.data.get("stats"):
                stats = serializer.data["stats"]
                create_note(collection, CollectionNote.Level.INFO, "Spider stats", data=stats)

        with get_publisher() as client:
            client.publish({"collection_id": collection.pk}, routing_key="collection_closed")
            if upgraded_collection:
                client.publish({"collection_id": upgraded_collection.pk}, routing_key="collection_closed")
            if compiled_collection := collection.get_compiled_collection():
                client.publish({"collection_id": compiled_collection.pk}, routing_key="collection_closed")

        return Response(status=status.HTTP_202_ACCEPTED)

    @extend_schema(responses={202: None})
    def destroy(self, request, pk=None):
        """Publish a message to RabbitMQ to wipe the dataset."""
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
        """Return the compiled collection's metadata."""
        compiled_collection = get_object_or_404(Collection, pk=pk)
        collection = compiled_collection.get_root_parent()

        if compiled_collection.transform_type != Collection.Transform.COMPILE_RELEASES:
            return Response("The collection must be a compiled collection", status=status.HTTP_400_BAD_REQUEST)

        metadata = {}

        with connection.cursor() as cursor:
            cursor.execute(
                """\
                SELECT
                    LEFT(MAX(ocid), 11) AS ocid_prefix,
                    MIN(release_date) AS published_from,
                    MAX(release_date) AS published_to
                FROM
                    compiled_release
                WHERE
                    collection_id = %(collection_id)s
                    AND release_date > '1970-01-01'
                    AND release_date <= %(today)s
                """,
                {"collection_id": pk, "today": str(datetime.datetime.now(tz=datetime.UTC).date())},
            )

            metadata.update(dictfetchone(cursor))

            cursor.execute(
                """\
                SELECT
                    data ->> 'license' AS license,
                    data ->> 'publicationPolicy' AS publication_policy
                FROM (
                    (
                        SELECT
                            data
                        FROM
                            package_data
                            JOIN record ON package_data_id = package_data.id
                        WHERE
                            collection_id = %(collection_id)s
                        LIMIT 1
                    )
                    UNION ALL
                    (
                        SELECT
                            data
                        FROM
                            package_data
                            JOIN release ON package_data_id = package_data.id
                        WHERE
                            collection_id = %(collection_id)s
                        LIMIT 1
                    )
                ) t
            """,
                {"collection_id": collection.id},
            )
            metadata.update(dictfetchone(cursor))

        return Response(metadata)

    @action(detail=True)
    def notes(self, request, pk=None):
        """Return the notes for the collection and its child collections."""
        root_collection = get_object_or_404(Collection, pk=pk)
        if root_collection.transform_type:
            return Response("The collection must be a root collection", status=status.HTTP_400_BAD_REQUEST)
        compiled_collection = root_collection.get_compiled_collection()
        upgraded_collection = root_collection.get_upgraded_collection()

        ids = [
            collection.id
            for collection in [root_collection, compiled_collection, upgraded_collection]
            if collection is not None
        ]

        notes_db = CollectionNote.objects.filter(collection_id__in=ids)
        notes = {level: [] for level in CollectionNote.Level.values}  # noqa: PD011
        for note in notes_db:
            notes[note.code].append([note.note, note.data])
        return Response(notes)

    @extend_schema(responses=TreeSerializer(many=True))
    @action(detail=True)
    def tree(self, request, pk=None):
        """Return the original collection and its derived collections, if any."""
        result = Collection.objects.raw(
            """\
            WITH RECURSIVE tree (
                id,
                transform_from_collection_id,
                root,
                deep
            ) AS (
                SELECT
                    collection.id,
                    collection.transform_from_collection_id,
                    id AS root,
                    1 AS deep
                FROM
                    collection
                WHERE
                    collection.transform_from_collection_id IS NULL
                UNION ALL
                SELECT
                    collection.id,
                    collection.transform_from_collection_id,
                    tree.root,
                    tree.deep + 1
                FROM
                    collection
                    JOIN tree ON collection.transform_from_collection_id = tree.id
            )
            SELECT
                collection.*
            FROM
                tree
                JOIN collection ON tree.id = collection.id
            WHERE
                tree.root = %(collection_id)s
            ORDER BY
                deep ASC
            """,
            {"collection_id": pk},
        )

        if not result:
            raise Http404

        serializer = TreeSerializer(result, many=True)
        return Response(serializer.data)
