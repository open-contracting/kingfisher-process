from django.core.exceptions import ValidationError
from django.test import TestCase

from process.models import (
    Collection,
    CollectionFile,
    CollectionNote,
    CompiledRelease,
    Data,
    PackageData,
    Record,
    Release,
)
from tests.fixtures import collection


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

        obj.clean_fields()  # no error

    def test_clean_fields_conditionally_mandatory(self):
        source = collection()
        source.save()

        values = [
            {"parent": source},
            {"transform_type": "compile-releases"},
        ]
        for kwargs in values:
            with self.subTest(kwargs=kwargs):
                obj = collection(**kwargs)
                with self.assertRaises(ValidationError) as e:
                    obj.clean_fields()

                message = "parent and transform_type must either be both set or both not set"
                self.assertEqual(e.exception.messages, [message])

    def test_clean_fields_deleted_at(self):
        source = collection(deleted_at="2001-01-01 00:00:00")
        source.save()

        obj = collection(parent=source, transform_type="compile-releases")
        with self.assertRaises(ValidationError) as e:
            obj.clean_fields()

        self.assertEqual(
            e.exception.message_dict,
            {
                "parent": [f"Parent collection {source.pk} is being deleted"],
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
                "transform_type": [f"Parent collection {compiled.pk} is compiled and can't be upgraded"],
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

        message = f"Parent collection {source.pk} is already transformed into {destination.pk}"
        self.assertEqual(e.exception.messages, [message])

    def test_get_compiled_collection_none(self):
        source = collection()
        source.save()

        self.assertIsNone(source.get_compiled_collection())

    def test_get_compiled_collection_none_upgrade(self):
        original = collection()
        original.save()

        upgraded = collection(parent=original, transform_type="upgrade-1-0-to-1-1")
        upgraded.save()

        self.assertIsNone(original.get_compiled_collection())

    def test_get_compiled_collection(self):
        source = collection()
        source.save()

        compiled = collection(parent=source, transform_type="compile-releases")
        compiled.save()

        self.assertEqual(source.get_compiled_collection(), compiled)

    def test_get_compiled_collection_upgrade(self):
        original = collection()
        original.save()

        upgraded = collection(parent=original, transform_type="upgrade-1-0-to-1-1")
        upgraded.save()

        compiled = collection(parent=upgraded, transform_type="compile-releases")
        compiled.save()

        self.assertEqual(original.get_compiled_collection(), compiled)
        self.assertEqual(upgraded.get_compiled_collection(), compiled)


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


class DataTests(TestCase):
    def test_str(self):
        obj = Data()
        self.assertEqual(str(obj), "{hash_md5} (id: {id})")

        obj.hash_md5 = "1bc29b36f623ba82aaf6724fd3b16718"
        self.assertEqual(str(obj), "1bc29b36f623ba82aaf6724fd3b16718 (id: {id})")


class PackageDataTests(TestCase):
    def test_str(self):
        obj = PackageData()
        self.assertEqual(str(obj), "{hash_md5} (id: {id})")

        obj.hash_md5 = "1bc29b36f623ba82aaf6724fd3b16718"
        self.assertEqual(str(obj), "1bc29b36f623ba82aaf6724fd3b16718 (id: {id})")


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
