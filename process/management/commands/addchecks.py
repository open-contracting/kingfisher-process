from django.core.management.base import CommandError
from django.utils.translation import gettext as t
from django.utils.translation import gettext_lazy as _

from process.cli import CollectionCommand
from process.models import Record, Release
from process.util import get_publisher
from process.util import wrap as w

routing_key = "addchecks"


class Command(CollectionCommand):
    help = w(t("Add processing steps to check data"))

    def handle_collection(self, collection, *args, **options):
        if collection.parent_id:
            raise CommandError(
                _("Collection %(id)s is not a root collection. Its parent is collection %(parent_id)s.")
                % collection.__dict__
            )

        with get_publisher() as client:
            for model in (Record, Release):
                qs = (
                    model.objects.filter(**{"collection": collection, f"{model.__name__.lower()}check__isnull": True})
                    .values_list("collection_file_item__collection_file", flat=True)
                    .distinct()
                )
                for collection_file_id in qs.iterator():
                    message = {"collection_id": collection.pk, "collection_file_id": collection_file_id}
                    client.publish(message, routing_key=routing_key)
