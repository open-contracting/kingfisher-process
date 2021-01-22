from django.urls import path
from django.conf.urls import url, include
from tastypie.api import Api

from process.views import api
from process.api.resources import CollectionResource
from process.api.resources import CollectionFileResource
from process.api.resources import CollectionFileItemResource

v1_api = Api(api_name='v1')
v1_api.register(CollectionResource())
v1_api.register(CollectionFileResource())
v1_api.register(CollectionFileItemResource())

urlpatterns = [
    path("api/create_collection", api.create_collection, name="create_collection"),
    path("api/close_collection", api.close_collection, name="close_collection"),
    path("api/create_collection_file", api.create_collection_file, name="create_collection_file"),
    url(r'^api/', include(v1_api.urls)),
]
