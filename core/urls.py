from django.urls import include, path

urlpatterns = [
    path("", include("process.urls"), name="process"),
]
