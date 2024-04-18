from django.conf import settings
from django.urls import include, path
from rest_framework import routers

from process.api import CollectionViewSet, TreeViewSet

router = routers.DefaultRouter()
router.register(r"collections", CollectionViewSet, basename="collection")
router.register(r"tree", TreeViewSet, basename="tree")

urlpatterns = [
    path("api-auth/", include("rest_framework.urls", namespace="rest_framework")),
    path(f"api/{settings.API_VERSION}/", include(router.urls)),
]
