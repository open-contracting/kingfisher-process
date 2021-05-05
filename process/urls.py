from django.conf import settings
from django.urls import include, path
from rest_framework import routers

from process.api import CollectionViewSet
from process.views import api

router = routers.DefaultRouter()
router.register(r'collections', CollectionViewSet)

urlpatterns = [
    path('api-auth/', include('rest_framework.urls', namespace='rest_framework')),
    path("api/{}/create_collection".format(settings.API_VERSION), api.create_collection, name="create_collection"),
    path("api/{}/close_collection".format(settings.API_VERSION), api.close_collection, name="close_collection"),
    path(
        "api/{}/create_collection_file".format(settings.API_VERSION),
        api.create_collection_file, name="create_collection_file"
    ),
    path("api/{}/".format(settings.API_VERSION), include(router.urls)),
]
