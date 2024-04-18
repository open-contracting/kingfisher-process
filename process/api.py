import json
import logging

from django.db import connection, transaction
from django.db.models import Case, Count, IntegerField, When
from django.db.models.functions import Now
from django.http.response import Http404
from django.shortcuts import get_object_or_404
from django_filters.rest_framework import DjangoFilterBackend
from rest_framework import serializers, viewsets, status
from rest_framework.decorators import action
from rest_framework.generics import ListAPIView, RetrieveAPIView
from rest_framework.response import Response

from core import settings
from process.models import Collection, ProcessingStep, CollectionNote
from process.processors.loader import create_collections
from process.util import get_publisher, create_note

logger = logging.getLogger(__name__)


class CollectionSerializer(serializers.ModelSerializer):
    steps_remaining_LOAD = serializers.SerializerMethodField()
    steps_remaining_UPGRADE = serializers.SerializerMethodField()
    steps_remaining_COMPILE = serializers.SerializerMethodField()
    steps_remaining_CHECK = serializers.SerializerMethodField()

    def get_steps_remaining_LOAD(self, obj):
        return obj.steps_remaining_LOAD

    def get_steps_remaining_UPGRADE(self, obj):
        return obj.steps_remaining_UPGRADE

    def get_steps_remaining_COMPILE(self, obj):
        return obj.steps_remaining_COMPILE

    def get_steps_remaining_CHECK(self, obj):
        return obj.steps_remaining_CHECK

    class Meta:
        model = Collection
        fields = [
            "source_id",
            "data_version",
            "sample",
            "steps",
            "options",
            "expected_files_count",
            "compilation_started",
            "parent",
            "transform_type",
            "data_type",
            "cached_releases_count",
            "cached_records_count",
            "cached_compiled_releases_count",
            "store_start_at",
            "store_end_at",
            "deleted_at",
            "completed_at",
            "steps_remaining_LOAD",
            "steps_remaining_UPGRADE",
            "steps_remaining_COMPILE",
            "steps_remaining_CHECK",
        ]


class CollectionViewSet(viewsets.ViewSetMixin, ListAPIView):
    queryset = (
        Collection.objects.annotate(
            steps_remaining_LOAD=Count(
                Case(
                    When(processing_steps__name=ProcessingStep.Name.LOAD, then=1),
                    output_field=IntegerField(),
                )
            )
        )
        .annotate(
            steps_remaining_UPGRADE=Count(
                Case(
                    When(processing_steps__name=ProcessingStep.Name.UPGRADE, then=1),
                    output_field=IntegerField(),
                )
            )
        )
        .annotate(
            steps_remaining_COMPILE=Count(
                Case(
                    When(processing_steps__name=ProcessingStep.Name.COMPILE, then=1),
                    output_field=IntegerField(),
                )
            )
        )
        .annotate(
            steps_remaining_CHECK=Count(
                Case(
                    When(processing_steps__name=ProcessingStep.Name.CHECK, then=1),
                    output_field=IntegerField(),
                )
            )
        )
    )

    serializer_class = CollectionSerializer
    filter_backends = [DjangoFilterBackend]
    filterset_fields = [
        "source_id",
        "data_version",
        "store_start_at",
        "store_end_at",
        "transform_type",
        "completed_at",
    ]

    def create(self, request):
        input_message = json.loads(request.data)
        if "source_id" not in input_message or "data_version" not in input_message:
            return Response(
                'Unable to parse input. Please provide {"source_id": "<source_id>", "data_version": "<data_version>"}',
                status=status.HTTP_400_BAD_REQUEST
            )

        if not settings.ENABLE_CHECKER and input_message.get("check"):
            logger.error("Checker is disabled in settings - see ENABLE_CHECKER value.")

        collection, upgraded_collection, compiled_collection = create_collections(
            # Identification
            input_message["source_id"],
            input_message["data_version"],
            sample=input_message.get("sample", False),
            # Steps
            upgrade=input_message.get("upgrade", False),
            compile=input_message.get("compile", False),
            check=input_message.get("check", False),
            # Other
            scrapyd_job=input_message.get("job", ""),
            note=input_message.get("note"),
        )

        result = {"collection_id": collection.pk}
        if upgraded_collection:
            result["upgraded_collection_id"] = upgraded_collection.pk
        if compiled_collection:
            result["compiled_collection_id"] = compiled_collection.pk

        return Response(result)

    @action(methods=["post"], detail=False)
    def close(self, request):
        input_message = json.loads(request.data)

        if "collection_id" not in input_message:
            return Response('Unable to parse input. Please provide {"collection_id": "<collection_id>"}',
                            status=status.HTTP_400_BAD_REQUEST)

        with transaction.atomic():
            collection = Collection.objects.select_for_update().get(pk=input_message["collection_id"])
            if input_message.get("stats"):
                # this value is used later on to detect, whether all collection has been processed yet
                collection.expected_files_count = input_message["stats"].get("kingfisher_process_expected_files_count",
                                                                             0)
            collection.store_end_at = Now()
            collection.save()

            upgraded_collection = collection.get_upgraded_collection()
            if upgraded_collection:
                upgraded_collection.expected_files_count = collection.expected_files_count
                upgraded_collection.store_end_at = Now()
                upgraded_collection.save()

            if input_message.get("reason"):
                create_note(collection, CollectionNote.Level.INFO, f"Spider close reason: {input_message['reason']}")

                if upgraded_collection:
                    create_note(
                        upgraded_collection, CollectionNote.Level.INFO,
                        f"Spider close reason: {input_message['reason']}"
                    )

            if input_message.get("stats"):
                create_note(collection, CollectionNote.Level.INFO, "Spider stats", data=input_message["stats"])

                if upgraded_collection:
                    create_note(
                        upgraded_collection, CollectionNote.Level.INFO, "Spider stats", data=input_message["stats"]
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

    def wipe(self, request):
        input_message = json.loads(request.data)
        if not input_message.get("collection_id"):
            return Response('Unable to parse input. Please provide {"collection_id":<some_number>}',
                            status=status.HTTP_400_BAD_REQUEST)

        with get_publisher() as client:
            client.publish(input_message, routing_key="wiper")

        return Response(status=status.HTTP_202_ACCEPTED)

    @action(detail=True)
    def metadata(self, request, pk=None):
        compiled_collection = get_object_or_404(Collection, id=pk)
        root_collection = compiled_collection.get_root_parent()
        meta_data = {}
        with connection.cursor() as cursor:
            # Data period
            cursor.execute("""\
            SELECT MAX(ocid) as ocid_prefix, MIN(data.data->>'date') as published_from,
                   MAX(data.data->>'date') as published_to
            FROM compiled_release
            JOIN data ON compiled_release.data_id = data.id
            WHERE
                compiled_release.collection_id = %(collection_id)s
                AND data.data ? 'date'
                AND data.data->>'date' <> ''
            """, {"collection_id": pk}, )
            compiled_release_metadata = dict(zip(["ocid_prefix", "published_from", "published_to"], cursor.fetchone()))
            if compiled_release_metadata["ocid_prefix"]:
                compiled_release_metadata["ocid_prefix"] = compiled_release_metadata["ocid_prefix"][:11]

            meta_data.update(compiled_release_metadata)

            # Publication policy and license
            cursor.execute("""\
                SELECT DATA->>'publicationPolicy' AS publication_policy,
                              DATA->>'license' AS license
                FROM package_data
                LEFT JOIN record r ON package_data.id = r.package_data_id
                AND r.collection_id = %(collection_id)s
                LEFT JOIN release r2 ON package_data.id = r2.package_data_id
                AND r2.collection_id = %(collection_id)s
                LIMIT 1
            """, {"collection_id": root_collection.id}, )

            meta_data.update(dict(zip(["publication_policy", "license"], cursor.fetchone())))

            return Response(meta_data)


class TreeSerializer(serializers.ModelSerializer):
    class Meta:
        model = Collection
        fields = "__all__"


class TreeViewSet(viewsets.ViewSetMixin, RetrieveAPIView):
    queryset = Collection.objects.filter(parent__isnull=True)

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

        serialized = TreeSerializer(result, many=True)

        return Response(serialized.data)
