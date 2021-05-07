from django_filters.rest_framework import DjangoFilterBackend
from rest_framework.generics import ListAPIView
from rest_framework.serializers import ModelSerializer
from rest_framework.viewsets import ViewSetMixin

from process.models import Collection


class CollectionSerializer(ModelSerializer):
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
                  "deleted_at"]


class CollectionViewSet(ViewSetMixin, ListAPIView):
    queryset = Collection.objects.all()
    serializer_class = CollectionSerializer
    filter_backends = [DjangoFilterBackend]
    filterset_fields = ["source_id", "data_version", "store_start_at", "store_end_at"]
