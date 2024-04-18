from django.conf import settings
from django.urls import include, path
from django.views.generic import TemplateView
from rest_framework import routers
from rest_framework.schemas import get_schema_view

from process.api import CollectionViewSet, TreeViewSet

router = routers.DefaultRouter()
router.register(r"collections", CollectionViewSet, basename="collection")
router.register(r"tree", TreeViewSet, basename="tree")

urlpatterns = [
    path("api-auth/", include("rest_framework.urls", namespace="rest_framework")),
    path(f"api/{settings.API_VERSION}/", include(router.urls)),
    # https://www.django-rest-framework.org/api-guide/schemas/#generating-a-dynamic-schema-with-schemaview
    path(
        "openapi",
        get_schema_view(
            title="API", description="Endpoints for managing collections in Kingfisher Process.", version="1.0.0"
        ),
        name="openapi-schema",
    ),
    # https://www.django-rest-framework.org/topics/documenting-your-api/#a-minimal-example-with-swagger-ui
    path(
        "swagger-ui/",
        TemplateView.as_view(template_name="swagger-ui.html", extra_context={"schema_url": "openapi-schema"}),
        name="swagger-ui",
    ),
]
