from django.conf import settings
from django.urls import include, path
from rest_framework import routers

from process.api import CollectionViewSet, TreeViewSet
from process.views import api

router = routers.DefaultRouter()
router.register(r"collections", CollectionViewSet)
router.register(r"tree", TreeViewSet, basename="tree")

urlpatterns = [
    path("api-auth/", include("rest_framework.urls", namespace="rest_framework")),
    path(f"api/{settings.API_VERSION}/create_collection", api.create_collection, name="create_collection"),
    path(f"api/{settings.API_VERSION}/close_collection", api.close_collection, name="close_collection"),
    path(f"api/{settings.API_VERSION}/wipe_collection", api.wipe_collection, name="wipe_collection"),
    path(f"api/{settings.API_VERSION}/", include(router.urls)),
]
