import datetime

from django.core.exceptions import ValidationError
from django.test import TestCase

from process.models import (
    Collection,
    CollectionFile,
    CollectionFileItem,
    CollectionNote,
    CompiledRelease,
    Data,
    PackageData,
    Record,
    Release,
)
from process.tests.fixtures import collection


class CollectionTests(TestCase):
    def test_str(self):
        obj = Collection()
        self.assertEqual(str(obj), "{source_id}:{data_version} (id: {id})")

        obj.source_id = "france"
        self.assertEqual(str(obj), "france:{data_version} (id: {id})")

        obj.data_version = "2001-01-01 00:00:00"
        self.assertEqual(str(obj), "france:2001-01-01 00:00:00 (id: {id})")

    def test_str_data_version(self):
        obj = Collection(data_version="2001-01-01 00:00:00")
        self.assertEqual(str(obj), "{source_id}:2001-01-01 00:00:00 (id: {id})")

    def test_clean_fields_existing_transform(self):
        source = collection()
        source.save()

        obj = collection(parent=source, transform_type="compile-releases")
        obj.save()

        try:
            obj.clean_fields()
        except Exception as e:
            self.fail("Unexpected exception {}".format(e))

    def test_clean_fields_conditionally_mandatory(self):
        source = collection()
        source.save()

        values = [
            dict(parent=source),
            dict(transform_type="compile-releases"),
        ]
        for kwargs in values:
            with self.subTest(kwargs=kwargs):
                obj = collection(**kwargs)
                with self.assertRaises(ValidationError) as e:
                    obj.clean_fields()

                message = "parent and transform_type must either be both set or both not set"
                self.assertEqual(e.exception.messages, [message])

    def test_add_step_check(self):
        source = collection()
        result = source.add_step("check")

        source.refresh_from_db()

        self.assertTrue(source.steps["check"])
        self.assertEqual(result, None)

    def test_add_step_transform(self):
        source = collection()

        for transform_type in ("compile-releases", "upgrade-1-0-to-1-1"):
            with self.subTest(transform_type=transform_type):
                result = source.add_step(transform_type)

                source.refresh_from_db()
                transforms = source.collection_set.filter(transform_type=transform_type)

                self.assertTrue(source.steps[transform_type])
                self.assertEqual(len(transforms), 1)
                self.assertEqual(transforms[0].source_id, "france")
                self.assertEqual(transforms[0].data_version, datetime.datetime(2001, 1, 1, 0, 0))
                self.assertFalse(transforms[0].sample)
                self.assertEqual(transforms[0].expected_files_count, None)
                self.assertEqual(transforms[0].parent_id, source.pk)
                self.assertEqual(transforms[0].transform_type, transform_type)
                self.assertEqual(result, transforms[0])

    def test_clean_fields_deleted_at(self):
        source = collection(deleted_at="2001-01-01 00:00:00")
        source.save()

        obj = collection(parent=source, transform_type="compile-releases")
        with self.assertRaises(ValidationError) as e:
            obj.clean_fields()

        self.assertEqual(
            e.exception.message_dict,
            {
                "parent": ["Parent collection {} is being deleted".format(source.pk)],
            },
        )

    def test_clean_fields_double_transform(self):
        source = collection()
        source.save()

        values = {
            "compile-releases": "Parent collection {} is itself already a compilation of {}",
            "upgrade-1-0-to-1-1": "Parent collection {} is itself already an upgrade of {}",
        }
        for transform_type, message in values.items():
            with self.subTest(transform_type=transform_type):
                original = collection(parent=source, transform_type=transform_type)
                original.save()

                obj = collection(parent=original, transform_type=transform_type)
                with self.assertRaises(ValidationError) as e:
                    obj.clean_fields()

                self.assertEqual(
                    e.exception.message_dict,
                    {
                        "transform_type": [message.format(original.pk, original.parent_id)],
                    },
                )

    def test_clean_fields_disallowed_transition(self):
        source = collection()
        source.save()

        compiled = collection(parent=source, transform_type="compile-releases")
        compiled.save()

        obj = collection(parent=compiled, transform_type="upgrade-1-0-to-1-1")
        with self.assertRaises(ValidationError) as e:
            obj.clean_fields()

        self.assertEqual(
            e.exception.message_dict,
            {
                "transform_type": ["Parent collection {} is compiled and can't be upgraded".format(compiled.pk)],
            },
        )

    def test_clean_fields_duplicate(self):
        source = collection()
        source.save()

        destination = collection(parent=source, transform_type="compile-releases")
        destination.save()

        obj = collection(parent=source, transform_type="compile-releases")
        with self.assertRaises(ValidationError) as e:
            obj.clean_fields()

        message = "Parent collection {} is already transformed into {}".format(source.pk, destination.pk)
        self.assertEqual(e.exception.messages, [message])


class CollectionNoteTests(TestCase):
    def test_str(self):
        obj = CollectionNote()
        self.assertEqual(str(obj), "{note} (id: {id})")

        obj.note = "A note"
        self.assertEqual(str(obj), "A note (id: {id})")


class CollectionFileTests(TestCase):
    def test_str(self):
        obj = CollectionFile()
        self.assertEqual(str(obj), "{filename} (id: {id})")

        obj.filename = "/path/to/file.json"
        self.assertEqual(str(obj), "/path/to/file.json (id: {id})")


class CollectionFileItemTests(TestCase):
    def test_str(self):
        obj = CollectionFileItem()
        self.assertEqual(str(obj), "step no. {number} (id: {id})")

        obj.number = 10
        self.assertEqual(str(obj), "step no. 10 (id: {id})")


class DataTests(TestCase):
    def test_str(self):
        obj = Data()
        self.assertEqual(str(obj), "{hash_md5} (id: {id})")

        obj.hash_md5 = "1bc29b36f623ba82aaf6724fd3b16718"
        self.assertEqual(str(obj), "1bc29b36f623ba82aaf6724fd3b16718 (id: {id})")


class PackageDataTests(TestCase):
    def test_str(self):
        obj = PackageData()
        self.assertEqual(str(obj), "")

        obj.hash_md5 = "1bc29b36f623ba82aaf6724fd3b16718"
        self.assertEqual(str(obj), "1bc29b36f623ba82aaf6724fd3b16718")


class ReleaseTests(TestCase):
    def test_str(self):
        obj = Release()
        self.assertEqual(str(obj), "{ocid}:{release_id} (id: {id})")

        obj.ocid = "ocds-213czf-000-00001"
        self.assertEqual(str(obj), "ocds-213czf-000-00001:{release_id} (id: {id})")

        obj.release_id = "ocds-213czf-000-00001-01-planning"
        self.assertEqual(str(obj), "ocds-213czf-000-00001:ocds-213czf-000-00001-01-planning (id: {id})")

    def test_str_release_id(self):
        obj = Release(release_id="ocds-213czf-000-00001-01-planning")
        self.assertEqual(str(obj), "{ocid}:ocds-213czf-000-00001-01-planning (id: {id})")


class RecordTests(TestCase):
    def test_str(self):
        obj = Record()
        self.assertEqual(str(obj), "{ocid} (id: {id})")

        obj.ocid = "ocds-213czf-000-00001"
        self.assertEqual(str(obj), "ocds-213czf-000-00001 (id: {id})")


class CompiledReleaseTests(TestCase):
    def test_str(self):
        obj = CompiledRelease()
        self.assertEqual(str(obj), "{ocid} (id: {id})")

        obj.ocid = "ocds-213czf-000-00001"
        self.assertEqual(str(obj), "ocds-213czf-000-00001 (id: {id})")
