from django.core.exceptions import ValidationError
from django.test import TestCase

from default.models import (Collection, CollectionFile, CollectionFileItem, CollectionNote, CompiledRelease, Data,
                            PackageData, Record, Release)

def collection(**kwargs):
    return Collection(
        source_id='example',
        data_version='2001-01-01 00:00:00',
        store_start_at='2001-01-01 00:00:00',
        **kwargs
    )


class CollectionTests(TestCase):
    def test_str(self):
        obj = Collection()
        self.assertEqual(str(obj), '{source_id}:{data_version}')

        obj.source_id = 'example'
        self.assertEqual(str(obj), 'example:{data_version}')

        obj.data_version = '2001-01-01 00:00:00'
        self.assertEqual(str(obj), 'example:2001-01-01 00:00:00')

    def test_str_data_version(self):
        obj = Collection(data_version='2001-01-01 00:00:00')
        self.assertEqual(str(obj), '{source_id}:2001-01-01 00:00:00')

    def test_clean_fields(self):
        obj = collection()
        obj.save()

        obj = collection(transform_from_collection_id=obj.id)
        with self.assertRaises(ValidationError) as e:
            obj.clean_fields()

        self.assertEqual(e.exception.message,
            'transform_from_collection_id and transform_type must either be both set or both not set.')


class CollectionNoteTests(TestCase):
    def test_str(self):
        obj = CollectionNote()
        self.assertEqual(str(obj), '')

        obj.note = 'A note'
        self.assertEqual(str(obj), 'A note')


class CollectionFileTests(TestCase):
    def test_str(self):
        obj = CollectionFile()
        self.assertEqual(str(obj), '')

        obj.filename = '/path/to/file.json'
        self.assertEqual(str(obj), '/path/to/file.json')

    def test_str_url(self):
        obj = CollectionFile(url='http://example.com/file.json')
        self.assertEqual(str(obj), 'http://example.com/file.json')


class CollectionFileItemTests(TestCase):
    def test_str(self):
        obj = CollectionFileItem()
        self.assertEqual(str(obj), '')

        obj.number = 10
        self.assertEqual(str(obj), '10')


class DataTests(TestCase):
    def test_str(self):
        obj = Data()
        self.assertEqual(str(obj), '')

        obj.hash_md5 = '1bc29b36f623ba82aaf6724fd3b16718'
        self.assertEqual(str(obj), '1bc29b36f623ba82aaf6724fd3b16718')


class PackageDataTests(TestCase):
    def test_str(self):
        obj = PackageData()
        self.assertEqual(str(obj), '')

        obj.hash_md5 = '1bc29b36f623ba82aaf6724fd3b16718'
        self.assertEqual(str(obj), '1bc29b36f623ba82aaf6724fd3b16718')


class ReleaseTests(TestCase):
    def test_str(self):
        obj = Release()
        self.assertEqual(str(obj), '{ocid}:{id}')

        obj.ocid = 'ocds-213czf-000-00001'
        self.assertEqual(str(obj), 'ocds-213czf-000-00001:{id}')

        obj.release_id = 'ocds-213czf-000-00001-01-planning'
        self.assertEqual(str(obj), 'ocds-213czf-000-00001:ocds-213czf-000-00001-01-planning')

    def test_str_release_id(self):
        obj = Release(release_id='ocds-213czf-000-00001-01-planning')
        self.assertEqual(str(obj), '{ocid}:ocds-213czf-000-00001-01-planning')


class RecordTests(TestCase):
    def test_str(self):
        obj = Record()
        self.assertEqual(str(obj), '')

        obj.ocid = 'ocds-213czf-000-00001'
        self.assertEqual(str(obj), 'ocds-213czf-000-00001')


class CompiledReleaseTests(TestCase):
    def test_str(self):
        obj = CompiledRelease()
        self.assertEqual(str(obj), '')

        obj.ocid = 'ocds-213czf-000-00001'
        self.assertEqual(str(obj), 'ocds-213czf-000-00001')
