import argparse
import sys

from django.core.management.base import CommandError
from django.utils.translation import gettext as t
from django.utils.translation import gettext_lazy as _

from process.management.commands.base.worker import BaseWorker
from process.models import Collection
from process.util import wrap as w


class Command(BaseWorker):
    help = w(t("""Wipes collection (and its ancestors) - COMPLETELY and IRREVERSIBLY.
                  Items in data and package_data tables remains untouched."""))

    worker_name = "wiper"

    def __init__(self):
        super().__init__(self.worker_name)

    def add_arguments(self, parser):
        parser.formatter_class = argparse.RawDescriptionHelpFormatter
        parser.add_argument(
            "-c",
            "--collection",
            help=_("the id of collection to wipe"),
        )

    def handle(self, *args, **options):
        if not options["collection"]:
            raise CommandError(_("Please indicate collection by its id."))

        try:
            collection_id = int(options["collection"])
        except ValueError:
            raise CommandError(_("--collection {} is not an int value").format(options["collection"]))

        try:
            collection = Collection.objects.get(id=options["collection"])
        except Collection.DoesNotExist:
            raise CommandError(_("A collection id: {} not found".format(collection_id)))

        confirm = self._get_input("Collection {} will be WIPED, confirm with Y: ".format(collection))
        if confirm != "Y":
            sys.exit()

        self._debug("Wiping collection {}".format(collection))

        collection.delete()

        self._info("Wipe command completed")
