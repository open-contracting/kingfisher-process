import argparse
import os
import time

from django.core.management.base import CommandError
from django.db import transaction
from django.db.models.functions import Now
from django.db.utils import IntegrityError
from django.utils.translation import gettext as t
from django.utils.translation import gettext_lazy as _

from process.management.commands.base.worker import BaseWorker
from process.models import Collection
from process.processors.loader import create_collection_file, create_master_collection
from process.scrapyd import configured
from process.util import json_dumps, walk
from process.util import wrap as w


class Command(BaseWorker):
    help = w(
        t(
            "Load data into a collection, asynchronously\n"
            "To load data into a new collection, set at least --source and --note. --time defaults to the earliest "
            "file modification time; if files were copied into place, set --time explicitly.\n"
            'The collection is automatically "closed" to new files.'
            "All files must have the same encoding (default UTF-8). If some files have different encodings, keep "
            "the collection open as above, and separately load the files with each encoding, using --encoding.\n"
            "The formats of files are automatically detected (release package, record package, release, record, "
            "compiled release), including JSON arrays and concatenated JSON of these. If OCDS data is embedded "
            "within files, use --root-path to indicate the path to the OCDS data to process within the files. For "
            'example, if release packages are in an array under a "results" key, use: --root-path results.item\n'
            "Additional processing is not automatically configured (checking, upgrading, merging, etc.). To add a "
            "pre-processing step, use the addstep command."
        )
    )

    worker_name = "loader"

    def __init__(self):
        super().__init__(self.worker_name)

    def add_arguments(self, parser):
        parser.formatter_class = argparse.RawDescriptionHelpFormatter
        parser.add_argument("PATH", help=_("a file or directory to load"), nargs="+", type=self.file_or_directory)
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
            help=_("whether the files represent a sample from the source, if loading into " "a new collection"),
            action="store_true",
        )
        parser.add_argument("--encoding", help=_("the encoding of all files (defaults to UTF-8)"))
        parser.add_argument("--root-path", help=_("the path to the OCDS data to process within all files"))
        parser.add_argument("-n", "--note", help=_("add a note to the collection (required for a new collection)"))
        parser.add_argument(
            "-f",
            "--force",
            help=_("use the provided --source value, regardless of whether it is " "recognized"),
            action="store_true",
        )
        parser.add_argument("-u", "--upgrade", help=_("upgrade collection to latest version"), action="store_true")
        parser.add_argument("-c", "--compile", help=_("compile collection"), action="store_true")
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
            collection, upgraded_collection = create_master_collection(
                options["source"],
                data_version,
                options["note"],
                upgrade=options["upgrade"],
                compile=options["upgrade"],
                sample=options["sample"],
                force=options["force"],
            )
        except IntegrityError:
            data = {
                "source_id": options["source"],
                "data_version": data_version,
                "sample": options["sample"],
            }
            collection = Collection.objects.get(**data, transform_type="")
            if collection.deleted_at:
                message = _("A collection %(id)s matching those arguments is being deleted")
            elif collection.store_end_at:
                message = _("A closed collection %(id)s matching those arguments already exists")
            else:
                message = _("An open collection %(id)s matching those arguments already exists.")
            raise CommandError(message % {"id": collection.pk})
        except ValueError as error:
            raise CommandError(error)

        self.debug("Processing path {}".format(options["PATH"]))

        for file_path in walk(options["PATH"]):
            with transaction.atomic():
                self.debug("Storing file {}".format(file_path))
                collection_file = create_collection_file(collection, file_path)

            message = {"collection_file_id": collection_file.id}

            self.publish(json_dumps(message))

        if not options["keep_open"]:
            collection.store_end_at = Now()
            collection.save()

            if upgraded_collection:
                upgraded_collection.store_end_at = Now()
                upgraded_collection.save()

        self.info("Load command completed")
