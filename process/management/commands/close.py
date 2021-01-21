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
            raise CommandError(_("--collection {} is not an int value").format(options["collection"]))

        try:
            collection = Collection.objects.get(id=collection_id)
        except Collection.DoesNotExist:
            raise CommandError(_("A collection id: {} not found".format(collection_id)))

        if collection.store_end_at:
            raise CommandError(
                _("A collection id: {} already closed at {}".format(options["collection"], collection.store_end_at))
            )

        if collection.parent:
            raise CommandError(
                _(
                    """A collection {} cannot be closed as its not
                    the "parent/root" collection, Hint: Its child of {}""".format(
                        collection, collection.parent
                    )
                )
            )

        collection.store_end_at = Now()
        collection.save()

        upgraded_collection = collection.get_upgraded_collection()

        if upgraded_collection:
            upgraded_collection.store_end_at = Now()
            upgraded_collection.save()

        self.info("Load command completed")
