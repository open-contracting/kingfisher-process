import os
import time
from argparse import RawDescriptionHelpFormatter

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

routing_key = "loader"


class Command(BaseCommand):
    help = w(
        t(
            "Load data into a new collection, asynchronously\n\n"
            "--time defaults to the earliest file modification time. If files were copied into place, set --time "
            "explicitly.\n\n"
            'The collection is automatically "closed" to new files. Use --keep-open to keep the collection open for '
            "future file additions.\n\n"
            "All files must have the same encoding (default UTF-8).\n\n"
            "The formats of files are automatically detected (release package, record package, release, record, "
            "compiled release), including JSON arrays and concatenated JSON of these.\n\n"
            "Additional processing is not automatically configured (upgrading, merging, checking, etc.). To add a "
            "step, use --upgrade, --compile and/or --check."
        )
    )

    def create_parser(self, prog_name, subcommand, **kwargs):
        return super().create_parser(prog_name, subcommand, formatter_class=RawDescriptionHelpFormatter, **kwargs)

    def add_arguments(self, parser):
        parser.add_argument("PATH", help=_("a file or directory to load"), nargs="+", type=file_or_directory)
        parser.add_argument(
            "-s",
            "--source",
            required=True,
            help=_("the source from which the files were retrieved (append '_local' if not sourced from Scrapy)"),
        )
        parser.add_argument(
            "-t",
            "--time",
            help=_(
                "the time at which the files were retrieved in 'YYYY-MM-DD HH:MM:SS' format "
                "(defaults to the earliest file modification time)"
            ),
        )
        parser.add_argument(
            "--sample", action="store_true", help=_("whether the files represent a sample from the source")
        )
        parser.add_argument("-n", "--note", required=True, help=_("a note to add to the collection"))
        parser.add_argument(
            "-f",
            "--force",
            action="store_true",
            help=_("use the provided --source value, regardless of whether it is recognized"),
        )
        parser.add_argument(
            "-u", "--upgrade", action="store_true", help=_("upgrade the collection to the latest OCDS version")
        )
        parser.add_argument(
            "-c", "--compile", action="store_true", help=_("create compiled releases from the collection")
        )
        parser.add_argument("-e", "--check", action="store_true", help=_("run structural checks on the collection"))
        parser.add_argument(
            "-k", "--keep-open", action="store_true", help=_("keep collection open for future file additions")
        )

    def handle(self, *args, **options):
        self.stderr.style_func = None

        if not configured() and not options["force"]:
            self.stderr.write(
                self.style.WARNING(
                    "The --source argument can't be validated, because a Scrapyd URL is not configured in settings.py."
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
                self.stderr.write(self.style.ERROR("Checker is disabled in settings - see ENABLE_CHECKER value."))

            collection, upgraded_collection, compiled_collection = create_collections(
                # Identification
                options["source"],
                data_version,
                sample=options["sample"],
                # Steps
                upgrade=options["upgrade"],
                compile=options["compile"],
                check=options["check"],
                # Other
                note=options["note"],
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
                    message = _("A matching collection %(id)s is being deleted. Try again later.")
                elif collection.store_end_at:
                    message = _(
                        "A matching closed collection %(id)s already exists. "
                        "Delete this collection, or change the --source or --time options."
                    )
                else:
                    message = _(
                        "A matching open collection %(id)s already exists. "
                        "Delete this collection, or change the --source or --time options."
                    )
                raise CommandError(message % {"id": collection.pk}) from error

            raise CommandError(error) from error

        self.stderr.write(f"Processing files: {' '.join(options['PATH'])}")

        with get_publisher() as client:
            for path in walk(options["PATH"]):
                # The transaction is inside the loop, since we can't rollback RabbitMQ messages, only PostgreSQL
                # statements. This ensures that any published message is paired with a database commit.
                with transaction.atomic():
                    self.stderr.write(f"Storing file: {path}")
                    collection_file = create_collection_file(collection, filename=path)

                message = {"collection_id": collection.pk, "collection_file_id": collection_file.pk}
                client.publish(message, routing_key=routing_key)

        if not options["keep_open"]:
            collection.store_end_at = Now()
            collection.save(update_fields=["store_end_at"])

            if upgraded_collection:
                upgraded_collection.store_end_at = Now()
                upgraded_collection.save(update_fields=["store_end_at"])

        self.stderr.write(self.style.SUCCESS("Done"))
