import json
from collections import OrderedDict
from unittest.mock import patch

from django.test import SimpleTestCase, TestCase, override_settings
from ocdskit.upgrade import upgrade_10_11

from process.models import CollectionNote, Data
from process.util import create_logger_note, get_or_create


class UtilTests(SimpleTestCase):
    @patch("process.util.create_note")
    def test_create_logger_note(self, create_note):
        with create_logger_note("collection", "ocdskit"):
            upgrade_10_11(
                json.loads(
                    '{"tender":{"tenderers":[{"name":"Acme Inc.","identifier":{"id":1}}]},"awards":[{"suppliers":'
                    '[{"name":"Acme Inc.","identifier":{"id":1},"additionalIdentifiers":[{"id":"a"}]}]}]}',
                    object_pairs_hook=OrderedDict,
                )
            )

        create_note.assert_called_once_with(
            "collection",
            CollectionNote.Level.WARNING,
            'party in "supplier" role differs from party in ["tenderer"] roles:\n'
            '{"id": "3c9756cf8983b14066a034079aa7aae4", "name": "Acme Inc.", "identifier": {"id": 1}, '
            '"additionalIdentifiers": [{"id": "a"}]}\n'
            '{"id": "3c9756cf8983b14066a034079aa7aae4", "name": "Acme Inc.", "identifier": {"id": 1}}\n',
        )

    @patch("process.util.create_note")
    def test_create_logger_note_not_called(self, create_note):
        with create_logger_note("collection", "ocdskit"):
            upgrade_10_11({})

        create_note.assert_not_called()


@override_settings(DEDUPLICATE_DATA=True)
class GetOrCreateDeduplicateTests(TestCase):
    def test_reuses_row_for_identical_data(self):
        first = get_or_create(Data, {"ocid": "ocds-1", "value": 1})
        second = get_or_create(Data, {"value": 1, "ocid": "ocds-1"})

        self.assertEqual(first.pk, second.pk)
        self.assertEqual(Data.objects.count(), 1)
        self.assertTrue(first.hash_md5)

    def test_creates_row_for_different_data(self):
        first = get_or_create(Data, {"ocid": "ocds-1"})
        second = get_or_create(Data, {"ocid": "ocds-2"})

        self.assertNotEqual(first.pk, second.pk)
        self.assertEqual(Data.objects.count(), 2)
        self.assertTrue(first.hash_md5)
        self.assertTrue(second.hash_md5)


@override_settings(DEDUPLICATE_DATA=False)
class GetOrCreateNoDeduplicateTests(TestCase):
    def test_always_creates_row(self):
        first = get_or_create(Data, {"ocid": "ocds-1"})
        second = get_or_create(Data, {"ocid": "ocds-1"})

        self.assertNotEqual(first.pk, second.pk)
        self.assertEqual(Data.objects.count(), 2)
        self.assertEqual(first.hash_md5, "")
        self.assertEqual(second.hash_md5, "")
