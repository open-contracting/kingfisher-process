import argparse

from django.core.management.base import CommandError
from django.db.models.functions import Now
from django.utils.translation import gettext as t
from django.utils.translation import gettext_lazy as _

from process.management.commands.base.worker import BaseWorker
from process.models import Collection
from process.util import wrap as w


class Command(BaseWorker):
    help = w(t("Close open collection."))

    worker_name = "loader"

    def __init__(self):
        super().__init__(self.worker_name)

    def add_arguments(self, parser):
        parser.formatter_class = argparse.RawDescriptionHelpFormatter
        parser.add_argument(
            "-c",
            "--collection",
            help=_("the id of collection which should be closed"),
        )

    def handle(self, *args, **options):
        if not options["collection"]:
            raise CommandError(_("Please indicate collection by its id."))

        try:
            collection_id = int(options["collection"])
        except ValueError:
            raise CommandError(_("--collection %(id)s is not an int value") % {"id": options["collection"]})

        try:
            collection = Collection.objects.get(pk=collection_id)
        except Collection.DoesNotExist:
            raise CommandError(_("Collection id=%(id)s not found") % {"id": collection_id})

        if collection.store_end_at:
            raise CommandError(_("Collection id=%(id)s already closed at %(store_end_at)s") % collection.__dict__)

        if collection.parent_id:
            raise CommandError(
                _(
                    "Collection %(child)s cannot be closed as it's not the parent/root collection."
                    "It's a child of %(parent)s."
                )
                % {"child": collection, "parent": collection.parent}
            )

        collection.store_end_at = Now()
        collection.save()

        upgraded_collection = collection.get_upgraded_collection()

        if upgraded_collection:
            upgraded_collection.store_end_at = Now()
            upgraded_collection.save()

        self.logger.info("Load command completed")
