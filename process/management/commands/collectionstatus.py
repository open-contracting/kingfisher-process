from django.core.management.base import CommandError
from django.utils.translation import gettext as t
from django.utils.translation import gettext_lazy as _

from process.cli import CollectionCommand
from process.management.commands.compiler import compilable
from process.management.commands.finisher import completable
from process.util import wrap as w


class Command(CollectionCommand):
    help = w(t("Get the status of a root collection and its children"))

    def handle_collection(self, collection, *args, **options):
        if collection.parent_id:
            raise CommandError(
                _("Collection %(id)s is not a root collection. Its parent is collection %(parent_id)s.")
                % collection.__dict__
            )

        # Fields
        print(f"steps: {collection.steps}")
        print(f"data_type: {collection.data_type}")
        print(f"store_end_at: {collection.store_end_at}")
        print(f"completed_at: {collection.completed_at}")
        print(f"expected_files_count: {collection.expected_files_count}")

        # Relations
        print(f"collection_files: {collection.collection_files.count()}")
        print(f"processing_steps: {collection.processing_steps.exists()}")

        # Logic
        print(f"compilable: {compilable(collection)}")
        print(f"completable: {completable(collection)}")

        compiled_collection = collection.get_compiled_collection()
        if compiled_collection:
            print()

            # Fields
            print(f"compilation_started: {compiled_collection.compilation_started}")

            # Relations
            print(f"collection_files: {compiled_collection.collection_files.count()}")

            # Logic
            print(f"completable: {completable(compiled_collection)}")
