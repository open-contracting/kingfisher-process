import logging

from django.test import TransactionTestCase
from django.utils import timezone

from process.management.commands.finisher import completable
from process.models import Collection, CollectionFile

logging.getLogger("process.management.commands.finisher").setLevel(logging.DEBUG)


class CompletableRecordPackagesTests(TransactionTestCase):
    fixtures = ["tests/fixtures/complete_db.json"]

    def setUp(self):
        self.parent_collection = Collection.objects.create(
            source_id="test_record_package",
            data_version="2023-01-01T00:00:00Z",
            data_type={"format": "record package", "array": False, "concatenated": False},
            store_end_at=timezone.now(),
            steps=["compile"],
        )

        self.compiled_collection = Collection.objects.create(
            source_id="test_record_package",
            data_version="2023-01-01T00:00:00Z",
            parent=self.parent_collection,
            transform_type=Collection.Transform.COMPILE_RELEASES,
            compilation_started=True,
            store_end_at=None,
        )

        self.file1 = CollectionFile.objects.create(collection=self.parent_collection, filename="test_file_1.json")
        self.file2 = CollectionFile.objects.create(collection=self.parent_collection, filename="test_file_2.json")

    def test_record_package_compilation_incomplete(self):
        """Test that "record package" collections are not completable if compilation is incomplete."""
        self.file1.compilation_started = True
        self.file1.save()

        self.assertFalse(completable(self.compiled_collection))

    def test_record_package_compilation_complete(self):
        """Test that "record package" collections are completable when all files have started compilation."""
        self.file1.compilation_started = True
        self.file1.save()

        self.file2.compilation_started = True
        self.file2.save()

        self.assertTrue(completable(self.compiled_collection))
