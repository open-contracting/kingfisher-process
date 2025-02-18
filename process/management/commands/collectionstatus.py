from argparse import RawDescriptionHelpFormatter

from django.core.management.base import CommandError
from django.utils.translation import gettext as t
from django.utils.translation import gettext_lazy as _

from process.cli import CollectionCommand
from process.management.commands.compiler import compilable
from process.management.commands.finisher import completable
from process.models import CollectionNote
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

    def bool_to_str(self, boolean):
        if not boolean:
            return self.style.WARNING("no (or not yet)")
        return "yes"

    def warn_none(self, value):
        if value is None:
            return self.style.WARNING(str(value))
        return value

    def warn_zero(self, value):
        if value == 0:
            return self.style.WARNING(str(value))
        return value

    def warn_nonzero(self, value):
        if value > 0:
            return self.style.WARNING(str(value))
        return value

    def handle_collection(self, collection, *args, **options):
        if collection.parent_id:
            raise CommandError(
                _("Collection %(id)s is not a root collection. Its parent is collection %(parent_id)s.")
                % collection.__dict__
            )

        if collection.data_type:
            data_type = collection.data_type["format"]
            if collection.data_type["array"]:
                data_type = f"a JSON array of {data_type}s"

            if collection.data_type["concatenated"]:
                data_type = f"concatenated JSON, starting with {data_type}"
        else:
            data_type = self.style.WARNING("to be determined")

        # Fields
        self.stdout.write(f"steps: {', '.join(collection.steps)}")
        self.stdout.write(f"data_type: {data_type}")
        self.stdout.write(f"store_end_at: {self.warn_none(collection.store_end_at)}")
        self.stdout.write(f"completed_at: {self.warn_none(collection.completed_at)}")
        self.stdout.write(f"expected_files_count: {collection.expected_files_count}")

        # Relations
        self.stdout.write(f"collection_files: {self.warn_zero(collection.collectionfile_set.count())}")
        self.stdout.write(f"processing_steps: {self.warn_nonzero(collection.processing_steps.count())}")

        compiled_collection = collection.get_compiled_collection()

        # Logic
        if not compiled_collection or not compiled_collection.compilation_started:
            self.stdout.write(f"compilable: {self.bool_to_str(compilable(collection))}")
        if not collection.completed_at:
            self.stdout.write(f"completable: {self.bool_to_str(completable(collection))}")

        # Notes
        notes = collection.collectionnote_set.filter(code=CollectionNote.Level.ERROR).all()
        if notes:
            self.stdout.write("Error-level collection notes:")
            for note in notes:
                self.stdout.write(self.style.ERROR(f"- {note.note} ({note.data})"))

        if compiled_collection:
            self.stdout.write("\nCompiled collection")

            # Fields
            self.stdout.write(f"compilation_started: {compiled_collection.compilation_started}")
            self.stdout.write(f"store_end_at: {self.warn_none(compiled_collection.store_end_at)}")
            self.stdout.write(f"completed_at: {self.warn_none(compiled_collection.completed_at)}")

            # Relations
            self.stdout.write(f"collection_files: {self.warn_zero(compiled_collection.collectionfile_set.count())}")
            self.stdout.write(f"processing_steps: {self.warn_nonzero(compiled_collection.processing_steps.count())}")

            # Logic
            if not compiled_collection.completed_at:
                self.stdout.write(f"completable: {self.bool_to_str(completable(compiled_collection))}")
