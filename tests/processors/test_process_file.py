from django.test import TransactionTestCase, override_settings
from ocdskit.exceptions import UnknownFormatError
from ocdskit.util import Format

from process.exceptions import EmptyFormatError, UnsupportedFormatError
from process.management.commands.file_worker import process_file, set_data_type
from process.models import CollectionFile, CompiledRelease, Data, PackageData, Record, Release
from tests.fixtures import collection


class DetectFormatTests(TransactionTestCase):
    def test_empty_format(self):
        source = collection()
        source.save()

        collection_file = CollectionFile(collection=source, filename="tests/fixtures/detect-format_empty.json")

        with self.assertRaisesRegex(
            EmptyFormatError,
            r"^Empty format 'empty package' for file tests/fixtures/detect-format_empty\.json \(id: {id}\)\.$",
        ):
            set_data_type(source, collection_file)

        self.assertEqual(source.data_type, {})

    def test_unsupported_format(self):
        source = collection()
        source.save()

        collection_file = CollectionFile(collection=source, filename="tests/fixtures/detect-format_versioned.json")

        with self.assertRaisesRegex(
            UnsupportedFormatError,
            r"^Unsupported format 'versioned release' for file tests/fixtures/detect-format_versioned.json "
            r"\(id: {id}\). Must be one of: compiled release, record package, release package\.$",
        ):
            set_data_type(source, collection_file)

        self.assertEqual(source.data_type, {})

    def test_unknown_format(self):
        source = collection()
        source.save()

        collection_file = CollectionFile(collection=source, filename="tests/fixtures/detect-format_object.json")

        with self.assertRaisesRegex(UnknownFormatError, r"^top-level JSON value is a non-OCDS object$"):
            set_data_type(source, collection_file)

        self.assertEqual(source.data_type, {})

    def test_file_not_found(self):
        source = collection()
        source.save()

        collection_file = CollectionFile(collection=source, filename="tests/fixtures/detect-format_nonexistent.json")

        with self.assertRaises(FileNotFoundError) as e:
            set_data_type(collection_file.collection, collection_file)

        self.assertEqual(
            str(e.exception), "[Errno 2] No such file or directory: 'tests/fixtures/detect-format_nonexistent.json'"
        )


class ProcessFileTests(TransactionTestCase):
    fixtures = ["tests/fixtures/complete_db.json"]

    def test_happy_day(self):
        collection_file = CollectionFile.objects.get(pk=1)
        collection_file.filename = "tests/fixtures/collection_file.json"
        collection_file.save()

        Release.objects.filter(collection_file=collection_file).delete()
        CollectionFile.objects.get(pk=3).delete()

        collection_file = CollectionFile.objects.select_related("collection").get(pk=1)
        upgraded_collection_file_id = process_file(collection_file)

        upgraded_collection_file = CollectionFile.objects.get(pk=upgraded_collection_file_id)

        self.assertEqual(upgraded_collection_file.filename, collection_file.filename)
        self.assertEqual(upgraded_collection_file.filename, "tests/fixtures/collection_file.json")
        self.assertEqual(upgraded_collection_file.collection.parent.id, collection_file.collection.id)

        self.assertEqual(PackageData.objects.filter(release__collection_file=collection_file).distinct().count(), 1)
        self.assertEqual(
            PackageData.objects.filter(release__collection_file=upgraded_collection_file).distinct().count(), 1
        )

        self.assertEqual(Release.objects.filter(collection_file=collection_file).count(), 100)
        self.assertEqual(Release.objects.filter(collection_file=upgraded_collection_file).count(), 100)

        self.assertEqual(upgraded_collection_file.filename, "tests/fixtures/collection_file.json")
        self.assertEqual(upgraded_collection_file.filename, "tests/fixtures/collection_file.json")
        self.assertEqual(upgraded_collection_file.filename, "tests/fixtures/collection_file.json")
        self.assertEqual(upgraded_collection_file.filename, "tests/fixtures/collection_file.json")


@override_settings(DEDUPLICATE_DATA=False)
class ProcessFileWithoutDeduplicationTests(TransactionTestCase):
    def test_bulk_store_release_package(self):
        for index, batch_size in enumerate((1, 30, 1000)):
            with self.subTest(batch_size=batch_size), override_settings(BULK_CREATE_BATCH_SIZE=batch_size):
                source = collection()
                source.data_version = f"2001-01-0{index + 1} 00:00:00"
                source.data_type = {"format": Format.release_package, "concatenated": False, "array": True}
                source.save()

                collection_file = CollectionFile(collection=source, filename="tests/fixtures/collection_file.json")
                collection_file.save()

                process_file(collection_file)

                releases = Release.objects.filter(collection_file=collection_file).select_related("data")
                package_data_ids = {release.package_data_id for release in releases}
                data_ids = {release.data_id for release in releases}

                self.assertEqual(releases.count(), 100)
                self.assertEqual(len(package_data_ids), 1)
                self.assertEqual(len(data_ids), 100)
                self.assertTrue(len({release.data.data["ocid"] for release in releases}), 100)

                package_data = PackageData.objects.get(pk=next(iter(package_data_ids)))

                self.assertEqual(releases.filter(package_data=package_data).count(), 100)
                self.assertNotIn("releases", package_data.data)

    def test_bulk_store_record_package(self):
        source = collection()
        source.data_type = {"format": Format.record_package, "concatenated": False, "array": True}
        source.save()

        collection_file = CollectionFile(collection=source, filename="tests/fixtures/record_package.json")
        collection_file.save()

        process_file(collection_file)

        records = Record.objects.filter(collection_file=collection_file)

        self.assertEqual(records.count(), 2)
        self.assertEqual(PackageData.objects.count(), 1)
        self.assertEqual(Data.objects.count(), 2)
        self.assertEqual(set(records.values_list("ocid", flat=True)), {"ocds-aaa111", "ocds-bbb222"})

        package_data = PackageData.objects.get()

        self.assertEqual(records.filter(package_data=package_data).count(), 2)
        self.assertNotIn("records", package_data.data)

    def test_bulk_store_compiled_release(self):
        source = collection()
        source.data_type = {"format": Format.compiled_release, "concatenated": True, "array": False}
        source.save()

        collection_file = CollectionFile(collection=source, filename="tests/fixtures/compiled_release.json")
        collection_file.save()

        process_file(collection_file)

        compiled_releases = CompiledRelease.objects.filter(collection_file=collection_file)

        self.assertEqual(compiled_releases.count(), 2)
        self.assertEqual(PackageData.objects.count(), 0)
        self.assertEqual(Data.objects.count(), 2)
        self.assertEqual(set(compiled_releases.values_list("ocid", flat=True)), {"ocds-aaa111", "ocds-bbb222"})
