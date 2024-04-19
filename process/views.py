import logging

from django.db import connection, transaction
from django.db.models.functions import Now
from django.http.response import Http404
from django.shortcuts import get_object_or_404
from rest_framework import serializers, status, viewsets
from rest_framework.decorators import action
from rest_framework.generics import RetrieveAPIView
from rest_framework.response import Response

from core import settings
from process.models import Collection, CollectionNote
from process.processors.loader import create_collections
from process.util import create_note, get_publisher

logger = logging.getLogger(__name__)


class CreateCollectionSerializer(serializers.Serializer):
    source_id = serializers.CharField(help_text="The spider name from Kingfisher Collect")
    data_version = serializers.CharField(help_text="The date when the collection's data was downloaded")
    check = serializers.BooleanField(help_text="Whether to run validation checks or not", required=False)
    compile = serializers.BooleanField(help_text="Whether to compile the collection's data or not", required=False)
    upgrade = serializers.BooleanField(
        help_text="Whether to upgrade the collection's data version or not", required=False
    )
    sample = serializers.BooleanField(help_text="Whether the collection contains only a sample", required=False)
    job = serializers.CharField(help_text="The job id", required=False)
    note = serializers.CharField(help_text="A note from the Kingfisher Collect crawl", required=False)


class CloseCollectionSerializer(serializers.Serializer):
    stats = serializers.JSONField(help_text="The statistics about the collection crawl", required=False)
    reason = serializers.CharField(help_text="The reason the crawl was finished", required=False)


class CollectionViewSet(viewsets.ViewSet):
    def create(self, request):
        serializer = CreateCollectionSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)

        if not settings.ENABLE_CHECKER and serializer.data.get("check"):
            logger.error("Checker is disabled in settings - see ENABLE_CHECKER value.")

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

    @action(methods=["post"], detail=True)
    def close(self, request, pk=None):
        serializer = CloseCollectionSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        get_object_or_404(Collection, id=pk)
        with transaction.atomic():
            collection = Collection.objects.select_for_update().get(pk=pk)
            if serializer.data.get("stats"):
                # this value is used later on to detect, whether all collection has been processed yet
                collection.expected_files_count = serializer.data["stats"].get(
                    "kingfisher_process_expected_files_count", 0
                )
            collection.store_end_at = Now()
            collection.save(update_fields=["expected_files_count", "store_end_at"])

            upgraded_collection = collection.get_upgraded_collection()
            if upgraded_collection:
                upgraded_collection.expected_files_count = collection.expected_files_count
                upgraded_collection.store_end_at = Now()
                upgraded_collection.save(update_fields=["expected_files_count", "store_end_at"])

            if serializer.data.get("reason"):
                create_note(collection, CollectionNote.Level.INFO, f"Spider close reason: {serializer.data['reason']}")
                if upgraded_collection:
                    create_note(
                        upgraded_collection,
                        CollectionNote.Level.INFO,
                        f"Spider close reason: {serializer.data['reason']}",
                    )

            if serializer.data.get("stats"):
                create_note(collection, CollectionNote.Level.INFO, "Spider stats", data=serializer.data["stats"])
                if upgraded_collection:
                    create_note(
                        upgraded_collection, CollectionNote.Level.INFO, "Spider stats", data=serializer.data["stats"]
                    )

        with get_publisher() as client:
            message = {"collection_id": collection.pk}
            client.publish(message, routing_key="collection_closed")

            if upgraded_collection:
                message = {"collection_id": upgraded_collection.pk}
                client.publish(message, routing_key="collection_closed")

            if compiled_collection := collection.get_compiled_collection():
                message = {"collection_id": compiled_collection.pk}
                client.publish(message, routing_key="collection_closed")

        return Response(status=status.HTTP_202_ACCEPTED)

    def destroy(self, request, pk=None):
        with get_publisher() as client:
            client.publish({"collection_id": pk}, routing_key="wiper")

        return Response(status=status.HTTP_202_ACCEPTED)

    @action(detail=True)
    def metadata(self, request, pk=None):
        compiled_collection = get_object_or_404(Collection, id=pk)
        root_collection = compiled_collection.get_root_parent()
        if compiled_collection.transform_type != Collection.Transform.COMPILE_RELEASES:
            return Response("The collection id must be a compiled collection", status=status.HTTP_400_BAD_REQUEST)
        meta_data = {}
        with connection.cursor() as cursor:
            # Data period
            cursor.execute(
                """\
            SELECT MAX(ocid) as ocid_prefix, MIN(data.data->>'date') as published_from,
                   MAX(data.data->>'date') as published_to
            FROM compiled_release
            JOIN data ON compiled_release.data_id = data.id
            WHERE
                compiled_release.collection_id = %(collection_id)s
                AND data.data ? 'date'
                AND data.data->>'date' <> ''
            """,
                {"collection_id": pk},
            )
            compiled_release_metadata = dict(zip(["ocid_prefix", "published_from", "published_to"], cursor.fetchone()))
            if compiled_release_metadata["ocid_prefix"]:
                compiled_release_metadata["ocid_prefix"] = compiled_release_metadata["ocid_prefix"][:11]

            meta_data.update(compiled_release_metadata)

            # Publication policy and license
            cursor.execute(
                """\
                SELECT DATA->>'publicationPolicy' AS publication_policy,
                              DATA->>'license' AS license
                FROM package_data
                LEFT JOIN record r ON package_data.id = r.package_data_id
                AND r.collection_id = %(collection_id)s
                LEFT JOIN release r2 ON package_data.id = r2.package_data_id
                AND r2.collection_id = %(collection_id)s
                LIMIT 1
            """,
                {"collection_id": root_collection.id},
            )

            meta_data.update(dict(zip(["publication_policy", "license"], cursor.fetchone())))

            return Response(meta_data)


class TreeSerializer(serializers.ModelSerializer):
    class Meta:
        model = Collection
        fields = "__all__"


class TreeViewSet(viewsets.ViewSetMixin, RetrieveAPIView):
    queryset = Collection.objects.filter(parent__isnull=True)
    serializer_class = TreeSerializer

    def retrieve(self, request, pk=None):
        result = Collection.objects.raw(
            """
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
