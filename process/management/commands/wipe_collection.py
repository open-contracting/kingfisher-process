from django.utils.translation import gettext as t

from process.management.commands.base.collection_command import CollectionCommand
from process.util import wrap as w


class Command(CollectionCommand):
    help = w(t("Deletes a collection and its ancestors. Rows in the package_data and data tables are not deleted."))

    def handle_collection(self, collection, *args, **options):
        confirm = input(f"Collection {collection} will be deleted. Do you want to continue? [y/N] ")

        if confirm.lower() == "y":
            print("Working... ", end="")
            collection.delete()
            print("success")
