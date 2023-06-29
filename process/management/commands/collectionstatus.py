from argparse import RawDescriptionHelpFormatter

from django.core.management.base import CommandError
from django.utils.translation import gettext as t
from django.utils.translation import gettext_lazy as _

from process.cli import CollectionCommand
from process.management.commands.compiler import compilable
from process.management.commands.finisher import completable
from process.util import wrap as w


class Command(CollectionCommand):
    help = w(
        t(
            "Get the status of a root collection and its children\n\n"
            "The output includes:\n\n"
            "steps:                 The steps that were run\n"
            "data_type:             A description of the data format\n"
            "store_end_at:          The time at which the collection ended\n"
            "completed_at:          The time at which the processing finished\n"
            "expected_files_count:  The number of files expected from Kingfisher Collect\n"
            "collection_files:      The number of related rows in the collection_file table\n"
            "processing_steps:      The number of individual processing steps remaining\n\n"
            "And if the original collection has a compiled collection:\n\n"
            "compilation_started:   Whether the compile step started\n"
            "completable:           Whether the collection can be marked as completed"
        )
    )

    def create_parser(self, prog_name, subcommand, **kwargs):
        return super().create_parser(prog_name, subcommand, formatter_class=RawDescriptionHelpFormatter, **kwargs)

    def handle_collection(self, collection, *args, **options):
        if collection.parent_id:
            raise CommandError(
                _("Collection %(id)s is not a root collection. Its parent is collection %(parent_id)s.")
                % collection.__dict__
            )

        data_type = collection.data_type["format"]
        if collection.data_type["array"]:
            data_type = f"a JSON array of {data_type}s"

        if collection.data_type["concatenated"]:
            data_type = f"concatenated JSON, starting with {data_type}"

        # Fields
        print(f"steps: {', '.join(collection.steps)}")
        print(f"data_type: {data_type}")
        print(f"store_end_at: {collection.store_end_at}")
        print(f"completed_at: {collection.completed_at}")
        print(f"expected_files_count: {collection.expected_files_count}")

        # Relations
        print(f"collection_files: {collection.collectionfile_set.count()}")
        print(f"processing_steps: {collection.processing_steps.count()}")

        compiled_collection = collection.get_compiled_collection()

        # Logic
        if not compiled_collection or not compiled_collection.compilation_started:
            print(f"compilable: {compilable(collection)}")
        if not collection.completed_at:
            print(f"completable: {completable(collection)}")

        if compiled_collection:
            print("\nCompiled collection")

            # Fields
            print(f"compilation_started: {compiled_collection.compilation_started}")
            print(f"store_end_at: {compiled_collection.store_end_at}")
            print(f"completed_at: {compiled_collection.completed_at}")

            # Relations
            print(f"collection_files: {compiled_collection.collectionfile_set.count()}")
            print(f"processing_steps: {compiled_collection.processing_steps.count()}")

            # Logic
            if not compiled_collection.completed_at:
                print(f"completable: {completable(compiled_collection)}")
