from django.urls import path

from process.views import api

urlpatterns = [
    path("api/create_collection", api.create_collection, name="create_collection"),
    path("api/close_collection", api.close_collection, name="close_collection"),
    path("api/create_collection_file", api.create_collection_file, name="create_collection_file"),
]
