from django.core.management.base import CommandError
from django.db import transaction
from django.utils.translation import gettext as t
from django.utils.translation import gettext_lazy as _

from process.cli import CollectionCommand
from process.processors.loader import create_collection_file, file_or_directory
from process.util import get_publisher, walk
from process.util import wrap as w

routing_key = "loader"


class Command(CollectionCommand):
    help = w(t("Load data into an open root collection, asynchronously"))

    def add_collection_arguments(self, parser):
        parser.add_argument("path", help=_("the file or directory to load"), nargs="+", type=file_or_directory)

    def handle_collection(self, collection, *args, **options):
        if collection.store_end_at:
            raise CommandError(
                _("Collection %(id)s is not an open collection. It was closed at %(store_end_at)s.")
                % collection.__dict__
            )

        if collection.parent_id:
            raise CommandError(
                _("Collection %(id)s is not a root collection. Its parent is collection %(parent_id)s.")
                % collection.__dict__
            )

        try:
            next(walk(options["path"]))
        except StopIteration:
            raise CommandError(_("No files to load"))

        with get_publisher() as client:
            for path in walk(options["path"]):
                print(f"Adding {path}")

                # The transaction is inside the loop, since we can't rollback RabbitMQ messages, only PostgreSQL
                # statements. This ensures that any published message is paired with a database commit.
                with transaction.atomic():
                    collection_file = create_collection_file(collection, file_path=path)

                message = {"collection_id": collection.pk, "collection_file_id": collection_file.pk}
                client.publish(message, routing_key=routing_key)

        print("done")
