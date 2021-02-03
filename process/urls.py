from django.conf.urls import include, url
from django.urls import path
from tastypie.api import Api

from process.api.resources import CollectionFileItemResource, CollectionFileResource, CollectionResource
from process.views import api

API_VERSION = "v1"
v1_api = Api(api_name=API_VERSION)
v1_api.register(CollectionResource())
v1_api.register(CollectionFileResource())
v1_api.register(CollectionFileItemResource())

urlpatterns = [
    path("api/{}/create_collection".format(API_VERSION), api.create_collection, name="create_collection"),
    path("api/{}/close_collection".format(API_VERSION), api.close_collection, name="close_collection"),
    path(
        "api/{}/create_collection_file".format(API_VERSION), api.create_collection_file, name="create_collection_file"
    ),
    url(r"^api/", include(v1_api.urls)),
]
