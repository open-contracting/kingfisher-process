from django.db.models import Case, Count, IntegerField, When
from django_filters.rest_framework import DjangoFilterBackend
from rest_framework.generics import ListAPIView
from rest_framework.serializers import ModelSerializer, SerializerMethodField
from rest_framework.viewsets import ViewSetMixin

from process.models import Collection, ProcessingStep


class CollectionSerializer(ModelSerializer):
    steps_remaining_LOAD = SerializerMethodField()
    steps_remaining_UPGRADE = SerializerMethodField()
    steps_remaining_COMPILE = SerializerMethodField()
    steps_remaining_CHECK = SerializerMethodField()

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
        fields = ["source_id",
                  "data_version",
                  "sample",
                  "steps",
                  "options",
                  "expected_files_count",
                  "compilation_started",
                  "check_older_data_with_schema_version_1_1",
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


class CollectionViewSet(ViewSetMixin, ListAPIView):
    queryset = Collection.objects.annotate(
                                    steps_remaining_LOAD=Count(
                                        Case(
                                            When(processing_steps__name=ProcessingStep.Types.LOAD, then=1),
                                            output_field=IntegerField(),
                                        )
                                    )).annotate(
                                    steps_remaining_UPGRADE=Count(
                                        Case(
                                            When(processing_steps__name=ProcessingStep.Types.UPGRADE, then=1),
                                            output_field=IntegerField(),
                                        )
                                    )).annotate(
                                    steps_remaining_COMPILE=Count(
                                        Case(
                                            When(processing_steps__name=ProcessingStep.Types.COMPILE, then=1),
                                            output_field=IntegerField(),
                                        )
                                    )).annotate(
                                    steps_remaining_CHECK=Count(
                                        Case(
                                           When(processing_steps__name=ProcessingStep.Types.CHECK, then=1),
                                           output_field=IntegerField(),
                                        )
                                    ))

    serializer_class = CollectionSerializer
    filter_backends = [DjangoFilterBackend]
    filterset_fields = ["source_id",
                        "data_version",
                        "store_start_at",
                        "store_end_at",
                        "transform_type",
                        "completed_at"]
