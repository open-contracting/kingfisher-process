from django.db.models import Case, Count, IntegerField, When
from django.http.response import Http404
from django_filters.rest_framework import DjangoFilterBackend
from rest_framework import serializers, viewsets
from rest_framework.generics import ListAPIView, RetrieveAPIView
from rest_framework.response import Response

from process.models import Collection, ProcessingStep


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
