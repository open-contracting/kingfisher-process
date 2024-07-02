from django.utils.translation import gettext as t
from django.utils.translation import gettext_lazy as _

from process.cli import CollectionCommand
from process.util import wrap as w


class Command(CollectionCommand):
    help = w(t("Delete a collection and its ancestors. Rows in the package_data and data tables are not deleted."))

    def add_collection_arguments(self, parser):
        parser.add_argument("-f", "--force", action="store_true", help=_("delete the collection(s) without prompting"))

    def handle_collection(self, collection, *args, **options):
        if not options["force"]:
            confirm = input(f"Collection {collection} will be deleted. Do you want to continue? [y/N] ")
            if confirm.lower() != "y":
                return

        if options["verbosity"] > 0:
            self.stderr.write("Working... ", ending="")

        collection.delete()

        if options["verbosity"] > 0:
            self.stderr.write(self.style.SUCCESS("done"))
