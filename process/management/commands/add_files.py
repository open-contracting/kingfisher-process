import argparse
import logging
import os

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from django.utils.translation import gettext as t
from django.utils.translation import gettext_lazy as _

from process.models import Collection
from process.processors.loader import create_collection_file, file_or_directory
from process.util import create_client, walk
from process.util import wrap as w

logger = logging.getLogger(__name__)
routing_key = "loader"


class Command(BaseCommand):
    help = w(t("Load data into a (open) collection, asynchronously"))

    def add_arguments(self, parser):
        parser.formatter_class = argparse.RawDescriptionHelpFormatter
        parser.add_argument("PATH", help=_("a file or directory to load"), nargs="+", type=file_or_directory)
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
            raise CommandError(_("--collection %(id)s is not an int value") % {"id": options["collection"]})

        # check whether data source exists
        mtimes = [os.path.getmtime(path) for path in walk(options["PATH"])]
        if not mtimes:
            raise CommandError(_("No files found"))

        try:
            collection = Collection.objects.get(pk=collection_id)
        except Collection.DoesNotExist:
            raise CommandError(_("Collection id=%(id)s not found") % {"id": collection_id})

        if collection.store_end_at:
            raise CommandError(_("Collection id=%(id)s already closed at %(store_end_at)s") % collection.__dict__)

        logger.debug("Processing path %s", options["PATH"])

        client = create_client()

        for file_path in walk(options["PATH"]):
            # note - keep transaction here, not "higher" around the whole cycle
            # we want to keep relation commited/published as close as possible
            with transaction.atomic():
                logger.debug("Storing file %s", file_path)
                collection_file = create_collection_file(collection, file_path)

            client.publish({"collection_file_id": collection_file.pk}, routing_key=routing_key)

        logger.info("Load command completed")
