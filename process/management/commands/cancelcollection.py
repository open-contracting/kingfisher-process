from django.db.models.functions import Now
from django.utils.translation import gettext as t

from process.cli import CollectionCommand
from process.util import wrap as w


class Command(CollectionCommand):
    help = w(t("Cancel all processing of a collection"))

    def handle_collection(self, collection, *args, **options):
        self.stderr.write(f"Working ({collection.pk})... ", ending="")

        cancel_collection(collection)
        if compiled_collection := collection.get_compiled_collection():
            cancel_collection(compiled_collection)
        if upgraded_collection := collection.get_upgraded_collection():
            cancel_collection(upgraded_collection)

        self.stderr.write(self.style.SUCCESS("done"))


def cancel_collection(collection):
    # Setting `deleted_at` causes messages to be acknowledged without processing.
    collection.deleted_at = Now()
    collection.save(update_fields=["deleted_at"])
