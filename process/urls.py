from django.conf import settings
from django.urls import include, path
from rest_framework import routers

from process import views
from process.api import CollectionViewSet, TreeViewSet

router = routers.DefaultRouter()
router.register(r"collections", CollectionViewSet)
router.register(r"tree", TreeViewSet, basename="tree")

urlpatterns = [
    path(f"api/{settings.API_VERSION}/create_collection", views.create_collection, name="create_collection"),
    path(f"api/{settings.API_VERSION}/close_collection", views.close_collection, name="close_collection"),
    path(f"api/{settings.API_VERSION}/wipe_collection", views.wipe_collection, name="wipe_collection"),
    path(f"api/{settings.API_VERSION}/", include(router.urls)),
]
