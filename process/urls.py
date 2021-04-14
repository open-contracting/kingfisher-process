from django.conf import settings
from django.urls import path

from process.views import api

urlpatterns = [
    path("api/{}/create_collection".format(settings.API_VERSION), api.create_collection, name="create_collection"),
    path("api/{}/close_collection".format(settings.API_VERSION), api.close_collection, name="close_collection"),
    path(
        "api/{}/create_collection_file".format(settings.API_VERSION),
        api.create_collection_file, name="create_collection_file"
    ),
]
