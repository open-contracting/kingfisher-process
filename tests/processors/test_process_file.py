from django.test import TransactionTestCase
from ocdskit.exceptions import UnknownFormatError

from process.exceptions import EmptyFormatError, UnsupportedFormatError
from process.management.commands.file_worker import process_file
from process.models import CollectionFile, CollectionFileItem, PackageData, Release
from tests.fixtures import collection


class DetectFormatTests(TransactionTestCase):
    def test_empty_format(self):
        source = collection()
        source.save()

        with self.assertRaisesRegex(
            EmptyFormatError,
            r"^Empty format 'empty package' for file tests/fixtures/detect-format_empty\.json \(id: {id}\)\.$",
        ):
            process_file(CollectionFile(collection=source, filename="tests/fixtures/detect-format_empty.json"))

        self.assertEqual(source.data_type, {"array": False, "format": "empty package", "concatenated": False})

    def test_unsupported_format(self):
        source = collection()
        source.save()

        with self.assertRaisesRegex(
            UnsupportedFormatError,
            r"^Unsupported format 'versioned release' for file tests/fixtures/detect-format_versioned.json "
            r"\(id: {id}\). Must be one of: compiled release, record package, release package\.$",
        ):
            process_file(CollectionFile(collection=source, filename="tests/fixtures/detect-format_versioned.json"))

        self.assertEqual(source.data_type, {"array": False, "format": "versioned release", "concatenated": False})

    def test_unknown_format(self):
        source = collection()
        source.save()

        with self.assertRaisesRegex(UnknownFormatError, r"^top-level JSON value is a non-OCDS object$"):
            process_file(CollectionFile(collection=source, filename="tests/fixtures/detect-format_object.json"))

        self.assertEqual(source.data_type, {})


class ProcessFileTests(TransactionTestCase):
    fixtures = ["tests/fixtures/complete_db.json"]

    def test_file_not_found(self):
        with self.assertRaises(FileNotFoundError) as e:
            collection_file = CollectionFile.objects.select_related("collection").get(pk=5)
            process_file(collection_file)
        self.assertEqual(str(e.exception), "[Errno 2] No such file or directory: 'ocds-px0z7d-10094-10001-1'")

    def test_happy_day(self):
        collection_file = CollectionFile.objects.get(pk=1)
        collection_file.filename = "tests/fixtures/collection_file.json"
        collection_file.save()

        CollectionFileItem.objects.filter(collection_file=collection_file).delete()
        CollectionFile.objects.get(pk=3).delete()

        collection_file = CollectionFile.objects.select_related("collection").get(pk=1)
        upgraded_collection_file_id = process_file(collection_file)

        upgraded_collection_file = CollectionFile.objects.get(pk=upgraded_collection_file_id)

        self.assertEqual(upgraded_collection_file.filename, collection_file.filename)
        self.assertEqual(upgraded_collection_file.filename, "tests/fixtures/collection_file.json")
        self.assertEqual(upgraded_collection_file.collection.parent.id, collection_file.collection.id)

        self.assertEqual(CollectionFileItem.objects.filter(collection_file=collection_file).count(), 1)
        self.assertEqual(CollectionFileItem.objects.filter(collection_file=upgraded_collection_file).count(), 1)

        self.assertEqual(
            PackageData.objects.filter(release__collection_file_item__collection_file=collection_file)
            .distinct()
            .count(),
            1,
        )
        self.assertEqual(
            PackageData.objects.filter(release__collection_file_item__collection_file=upgraded_collection_file)
            .distinct()
            .count(),
            1,
        )

        self.assertEqual(Release.objects.filter(collection_file_item__collection_file=collection_file).count(), 100)
        self.assertEqual(
            Release.objects.filter(collection_file_item__collection_file=upgraded_collection_file).count(), 100
        )

        self.assertEqual(upgraded_collection_file.filename, "tests/fixtures/collection_file.json")
        self.assertEqual(upgraded_collection_file.filename, "tests/fixtures/collection_file.json")
        self.assertEqual(upgraded_collection_file.filename, "tests/fixtures/collection_file.json")
        self.assertEqual(upgraded_collection_file.filename, "tests/fixtures/collection_file.json")
