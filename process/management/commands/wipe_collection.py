import logging
import sys

from django.core.management.base import BaseCommand, CommandError
from django.utils.translation import gettext as t
from django.utils.translation import gettext_lazy as _

from process.models import Collection
from process.util import wrap as w

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = w(
        t(
            "Wipes collection (and its ancestors) - COMPLETELY and IRREVERSIBLY. "
            "Items in data and package_data tables remains untouched."
        )
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "-c",
            "--collection",
            help=_("the ID of collection to wipe"),
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

        confirm = input("Collection {} will be WIPED, confirm with Y: ".format(collection))
        if confirm != "Y":
            sys.exit()

        logger.debug("Wiping collection %s", collection)

        collection.delete()

        logger.info("Wipe command completed")
