import logging
import os
import time

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from django.db.models.functions import Now
from django.utils.translation import gettext as t
from django.utils.translation import gettext_lazy as _

from process.models import Collection
from process.processors.loader import create_collection_file, create_collections, file_or_directory
from process.scrapyd import configured
from process.util import get_publisher, walk
from process.util import wrap as w

logger = logging.getLogger(__name__)
routing_key = "loader"


class Command(BaseCommand):
    help = w(
        t(
            "Load data into a collection, asynchronously\n"
            "To load data into a new collection, set at least --source and --note. --time defaults to the earliest "
            "file modification time; if files were copied into place, set --time explicitly.\n"
            'The collection is automatically "closed" to new files.'
            "All files must have the same encoding (default UTF-8).\n"
            "The formats of files are automatically detected (release package, record package, release, record, "
            "compiled release), including JSON arrays and concatenated JSON of these.\n"
            "Additional processing is not automatically configured (upgrading, merging, etc.). To add a "
            "pre-processing step, use --upgrade or --compile."
        )
    )

    def add_arguments(self, parser):
        parser.add_argument("PATH", help=_("a file or directory to load"), nargs="+", type=file_or_directory)
        parser.add_argument(
            "-s",
            "--source",
            help=_(
                "the source from which the files were retrieved, if loading into "
                'a new collection (please append "_local" if the data was not '
                "collected by Kingfisher Collect)"
            ),
        )
        parser.add_argument(
            "-t",
            "--time",
            help=_(
                "the time at which the files were retrieved, if loading into a new "
                'collection, in "YYYY-MM-DD HH:MM:SS" format (defaults to the '
                "earliest file modification time)"
            ),
        )
        parser.add_argument(
            "--sample",
            help=_("whether the files represent a sample from the source, if loading into a new collection"),
            action="store_true",
        )
        parser.add_argument("-n", "--note", help=_("add a note to the collection (required for a new collection)"))
        parser.add_argument(
            "-f",
            "--force",
            help=_("use the provided --source value, regardless of whether it is recognized"),
            action="store_true",
        )
        parser.add_argument("-u", "--upgrade", help=_("upgrade collection to latest version"), action="store_true")
        parser.add_argument("-c", "--compile", help=_("compile collection"), action="store_true")
        parser.add_argument("-e", "--check", help=_("check collection"), action="store_true")
        parser.add_argument(
            "-k", "--keep-open", help=_("keep collection open for future file additions"), action="store_true"
        )

    def handle(self, *args, **options):
        if not options["source"]:
            raise CommandError(
                _(
                    "Please indicate collection source (using --source and --note and, optionally, "
                    "--time and --sample)"
                )
            )

        if options["source"] and not options["note"]:
            raise CommandError(_("You must add a note (using --note) when loading into a new collection"))

        if not configured() and not options["force"]:
            self.stderr.write(
                self.style.WARNING(
                    "The --source argument can't be validated, because a Scrapyd URL "
                    "is not configured in settings.py."
                )
            )

        # create proper data_version
        mtimes = [os.path.getmtime(path) for path in walk(options["PATH"])]
        if not mtimes:
            raise CommandError(_("No files found"))

        data_version = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(min(mtimes)))
        if options["time"]:
            if options["time"] > data_version:
                raise CommandError(
                    _("%(time)r is greater than the earliest file modification time: %(mtime)r")
                    % {"time": options["time"], "mtime": data_version}
                )
            data_version = options["time"]

        try:
            if not settings.ENABLE_CHECKER and options["check"]:
                logger.error("Checker is disabled in settings - see ENABLE_CHECKER value.")

            collection, upgraded_collection, compiled_collection = create_collections(
                options["source"],
                data_version,
                options["note"],
                upgrade=options["upgrade"],
                compile=options["compile"],
                check=options["check"],
                sample=options["sample"],
                force=options["force"],
            )
        except ValueError as error:
            if str(error) == _("A matching collection already exists."):
                data = {
                    "source_id": options["source"],
                    "data_version": data_version,
                    "sample": options["sample"],
                }
                collection = Collection.objects.get(**data, transform_type="")
                if collection.deleted_at:
                    message = _("A collection %(id)s matching those arguments is being deleted.")
                elif collection.store_end_at:
                    message = _("A closed collection %(id)s matching those arguments already exists.")
                else:
                    message = _("An open collection %(id)s matching those arguments already exists.")
                raise CommandError(message % {"id": collection.pk})
            else:
                raise CommandError(error)

        logger.debug("Processing path %s", options["PATH"])

        with get_publisher() as client:
            for file_path in walk(options["PATH"]):
                # The transaction is inside the loop, since we can't rollback RabbitMQ messages, only PostgreSQL
                # statements. This ensures that any published message is paired with a database commit.
                with transaction.atomic():
                    logger.debug("Storing file %s", file_path)
                    collection_file = create_collection_file(collection, file_path=file_path)

                message = {"collection_id": collection.pk, "collection_file_id": collection_file.pk}
                client.publish(message, routing_key=routing_key)

        if not options["keep_open"]:
            collection.store_end_at = Now()
            collection.save()

            if upgraded_collection:
                upgraded_collection.store_end_at = Now()
                upgraded_collection.save()

            if compiled_collection:
                compiled_collection.store_end_at = Now()
                compiled_collection.save()

        logger.info("Load command completed")
