from django.core.management.base import CommandError
from django.db import transaction
from django.db.models.functions import Now
from django.utils.translation import gettext as t
from django.utils.translation import gettext_lazy as _

from process.cli import CollectionCommand
from process.util import get_publisher
from process.util import wrap as w

routing_key = "collection_closed"


class Command(CollectionCommand):
    help = w(t("Close an open root collection and its derived collections, if any"))

    def handle_collection(self, collection, *args, **options):
        if collection.parent_id:
            raise CommandError(
                _("Collection %(id)s is not a root collection. Its parent is collection %(parent_id)s.")
                % collection.__dict__
            )

        self.stderr.write("Working... ", ending="")

        if not collection.store_end_at:
            with transaction.atomic():
                collection.store_end_at = Now()
                collection.save(update_fields=["store_end_at"])

                if upgraded_collection := collection.get_upgraded_collection():
                    upgraded_collection.store_end_at = Now()
                    upgraded_collection.save(update_fields=["store_end_at"])

        with get_publisher() as client:
            client.publish({"collection_id": collection.pk}, routing_key=routing_key)
            if upgraded_collection:
                client.publish({"collection_id": upgraded_collection.pk}, routing_key=routing_key)
            if compiled_collection := collection.get_compiled_collection():
                client.publish({"collection_id": compiled_collection.pk}, routing_key=routing_key)

        self.stderr.write(self.style.SUCCESS("done"))
