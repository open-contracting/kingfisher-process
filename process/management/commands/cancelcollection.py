from django.utils.translation import gettext as t

from process.cli import CollectionCommand
from process.util import get_publisher
from process.util import wrap as w

routing_key = "collection_cancelled"


class Command(CollectionCommand):
    help = w(t("Cancel the finisher worker for a collection"))

    def handle_collection(self, collection, *args, **options):
        self.stderr.write("Working... ", ending="")

        with get_publisher() as client:
            client.publish({"collection_id": collection.pk, "cancel": True}, routing_key=routing_key)

        self.stderr.write(self.style.SUCCESS("done"))
