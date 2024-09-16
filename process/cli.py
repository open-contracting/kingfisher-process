import logging

from django.core.management.base import BaseCommand, CommandError
from django.utils.translation import gettext_lazy as _

from process.models import Collection


class CollectionCommand(BaseCommand):
    def add_arguments(self, parser):
        """Add default arguments to the command."""
        parser.add_argument("collection_id", help=_("the ID of the collection"))
        self.add_collection_arguments(parser)

    def add_collection_arguments(self, parser):
        """Add arguments specific to this command."""

    def handle(self, *args, **options):
        """Get the collection."""
        self.stderr.style_func = None

        logging.getLogger("process").setLevel(logging.DEBUG)

        collection_id = options["collection_id"]

        try:
            collection = Collection.objects.get(pk=collection_id)
        except Collection.DoesNotExist:
            raise CommandError(_("Collection %(id)s does not exist") % {"id": collection_id}) from None

        self.handle_collection(collection, *args, **options)

    def handle_collection(self, collection, *args, **options):
        """Run the command."""
        raise NotImplementedError("collection commands must implement handle_collection()")
