import argparse
import os

from django.core.management.base import CommandError
from django.db import transaction
from django.utils.translation import gettext as t
from django.utils.translation import gettext_lazy as _

from process.management.commands.base.worker import BaseWorker
from process.models import Collection
from process.processors.loader import create_collection_file
from process.util import json_dumps, walk
from process.util import wrap as w


class Command(BaseWorker):
    help = w(t("Load data into a (open) collection, asynchronously"))

    worker_name = "loader"

    def __init__(self):
        super().__init__(self.worker_name)

    def add_arguments(self, parser):
        parser.formatter_class = argparse.RawDescriptionHelpFormatter
        parser.add_argument("PATH", help=_("a file or directory to load"), nargs="+", type=self._file_or_directory)
        parser.add_argument(
            "-c",
            "--collection",
            help=_("the id of collection to which the files whould be loaded"),
        )

    def handle(self, *args, **options):
        if not options["collection"]:
            raise CommandError(_("Please indicate collection by its id to load data at."))

        try:
            collection_id = int(options["collection"])
        except ValueError:
            raise CommandError(_("--collection {} is not an int value").format(options["collection"]))

        # check whether data source exists
        mtimes = [os.path.getmtime(path) for path in walk(options["PATH"])]
        if not mtimes:
            raise CommandError(_("No files found"))

        try:
            collection = Collection.objects.get(id=options["collection"])
        except Collection.DoesNotExist:
            raise CommandError(_("A collection id: {} not found".format(collection_id)))

        if collection.store_end_at:
            raise CommandError(
                _("A collection id: {} already closed at {}".format(collection_id, collection.store_end_at))
            )

        self._debug("Processing path {}".format(options["PATH"]))

        for file_path in walk(options["PATH"]):
            # note - keep transaction here, not "higher" around the whole cycle
            # we want to keep relation commited/published as close as possible
            with transaction.atomic():
                self._debug("Storing file {}".format(file_path))
                collection_file = create_collection_file(collection, file_path)

            message = {"collection_file_id": collection_file.id}

            self._publish(json_dumps(message))

        self._info("Load command completed")
