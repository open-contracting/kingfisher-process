from django.conf import settings
from django.urls import include, path
from drf_spectacular import views
from rest_framework import routers

from process.api import CollectionViewSet, TreeViewSet

router = routers.DefaultRouter()
router.register(r"collections", CollectionViewSet, basename="collection")
router.register(r"tree", TreeViewSet, basename="tree")

urlpatterns = [
    path(f"api/{settings.API_VERSION}/", include(router.urls)),
    path("api/schema/", views.SpectacularAPIView.as_view(), name="schema"),
    path("api/schema/swagger-ui/", views.SpectacularSwaggerView.as_view(url_name="schema"), name="swagger-ui"),
    path("api/schema/redoc/", views.SpectacularRedocView.as_view(url_name="schema"), name="redoc"),
]
