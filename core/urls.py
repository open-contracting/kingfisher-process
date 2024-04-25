from django.urls import include, path

urlpatterns = [
    path("api/", include("process.urls"), name="process"),
]
