from django.urls import path
from drf_spectacular import views as drfviews
from rest_framework.routers import SimpleRouter

from process import views

router = SimpleRouter(use_regex_path=False)
router.register(r"collections", views.CollectionViewSet, basename="collection")

urlpatterns = [
    *router.urls,
    path("schema/", drfviews.SpectacularAPIView.as_view(), name="schema"),
    path("schema/swagger-ui/", drfviews.SpectacularSwaggerView.as_view(url_name="schema"), name="swagger-ui"),
    path("schema/redoc/", drfviews.SpectacularRedocView.as_view(url_name="schema"), name="redoc"),
]
