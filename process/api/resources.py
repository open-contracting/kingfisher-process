from django.db.models import F, Q
from tastypie import fields
from tastypie.resources import ALL_WITH_RELATIONS, ModelResource

from process.models import Collection, CollectionFile, CollectionFileItem


class BaseResource(ModelResource):
    def apply_filters(self, request, applicable_filters):
        if "custom" in applicable_filters:
            custom = applicable_filters.pop("custom")
        else:
            custom = None

        semi_filtered = super(BaseResource, self).apply_filters(request, applicable_filters)

        return semi_filtered.filter(custom) if custom else semi_filtered

    def build_filters(self, filters=None, **kwargs):
        if filters is None:
            filters = {}

        orm_filters = super(BaseResource, self).build_filters(filters, **kwargs)

        if "q" in filters:
            custom_filter = self.get_custom_filter(filters["q"])
            if custom_filter:
                orm_filters.update({"custom": custom_filter})

        return orm_filters

    def get_custom_filter(self, query):
        return None

    def build_filter_query(self, filters, queries):
        qset = None
        for query in queries:
            qtmp = None
            for f, q in query.items():
                if f in filters and filters[f]:
                    qtmp = q if qtmp is None else qtmp | q

            if qtmp:
                qset = qtmp if qset is None else qset & qtmp

        return qset

    def apply_sorting(self, obj_list, options=None):
        ordering = options.get("order_by", "")
        if options and ordering.startswith("-"):
            return obj_list.order_by(F(ordering[1:]).desc(nulls_last=True))

        return super(BaseResource, self).apply_sorting(obj_list, options)

    class Meta:
        abstract = True
        allowed_methods = ["get"]
        list_allowed_methods = ["get"]


class CollectionResource(BaseResource):
    def get_custom_filter(self, query):
        return Q(source_id__icontains=query) | Q(transform_type__icontains=query)

    def dehydrate(self, bundle):
        bundle.data["parent"] = bundle.obj.parent
        return bundle

    class Meta(BaseResource.Meta):
        abstract = False
        queryset = Collection.objects.all()

        resource_name = "collection"
        excludes = []
        filtering = {}
        ordering = ["id", "store_start_at", "source_id"]


class CollectionFileResource(BaseResource):
    def get_custom_filter(self, query):
        return Q(filename__icontains=query) | Q(url__icontains=query)

    collection = fields.ForeignKey(CollectionResource, "collection", full=False)

    class Meta(BaseResource.Meta):
        abstract = False
        queryset = CollectionFile.objects.all()
        resource_name = "collection-file"
        excludes = []
        filtering = {"collection": ALL_WITH_RELATIONS}
        ordering = ["id", "filename", "url"]


class CollectionFileItemResource(BaseResource):
    def get_custom_filter(self, query):
        return Q(number__icontains=query)

    collection_file = fields.ForeignKey(CollectionFileResource, "collection_file", full=False)

    class Meta(BaseResource.Meta):
        abstract = False
        queryset = CollectionFileItem.objects.all()
        resource_name = "collection-file-item"
        excludes = []
        filtering = {"collection_file": ALL_WITH_RELATIONS}
        ordering = ["id", "number"]
