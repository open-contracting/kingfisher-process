from django.core.management.base import CommandError
from django.db.models.functions import Now
from django.utils.translation import gettext as t
from django.utils.translation import gettext_lazy as _

from process.cli import CollectionCommand
from process.util import wrap as w


class Command(CollectionCommand):
    help = w(t("Close an open root collection."))

    def handle_collection(self, collection, *args, **options):
        if collection.store_end_at:
            raise CommandError(
                _("Collection %(id)s is not an open collection. It was closed at %(store_end_at)s.")
                % collection.__dict__
            )

        if collection.parent_id:
            raise CommandError(
                _("Collection %(id)s is not a root collection. Its parent is collection %(parent_id)s.")
                % collection.__dict__
            )

        print("Working... ", end="")

        collection.store_end_at = Now()
        collection.save()

        upgraded_collection = collection.get_upgraded_collection()
        if upgraded_collection:
            upgraded_collection.store_end_at = Now()
            upgraded_collection.save()

        print("done")
