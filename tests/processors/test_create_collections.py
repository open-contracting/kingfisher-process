from django.db.utils import IntegrityError
from django.test import TransactionTestCase

from process.models import Collection, CollectionNote
from process.processors.loader import create_collections


class CreateCollectionsTests(TransactionTestCase):
    fixtures = ["tests/fixtures/complete_db.json"]

    def test_malformed_input(self):
        with self.assertRaises(ValueError) as e:
            create_collections(
                "test", "wrong_data_version", "testing note", upgrade=True, compile=True, check=True, sample=False
            )
        self.assertEqual(
            str(e.exception),
            "data_version 'wrong_data_version' is not in \"YYYY-MM-DD HH:MM:SS\" format or is an invalid date/time",
        )

    def test_integrity_error(self):
        with self.assertRaises(IntegrityError) as e:
            create_collections(
                "portugal-releases", "2020-12-29 09:22:08", "testing note", upgrade=False, compile=False, sample=False
            )
        self.assertTrue(str(e.exception).startswith("duplicate key value violates unique constraint "))

    def test_happy_day(self):
        collection, upgraded_collection, compiled_collection = create_collections(
            "test", "2020-12-29 09:22:09", "testing note", upgrade=True, compile=True, check=True, sample=False
        )

        self.assertEqual(upgraded_collection.parent.id, collection.id)
        self.assertEqual(compiled_collection.parent.id, upgraded_collection.id)
        self.assertTrue("upgrade" in collection.steps)
        self.assertTrue("check" in collection.steps)
        self.assertTrue("compile" in upgraded_collection.steps)
        self.assertEqual(Collection.Transforms.UPGRADE_10_11, upgraded_collection.transform_type)
        self.assertEqual(Collection.Transforms.COMPILE_RELEASES, compiled_collection.transform_type)
        self.assertEqual("testing note", CollectionNote.objects.get(collection=collection).note)
        self.assertEqual("testing note", CollectionNote.objects.get(collection=upgraded_collection).note)
        self.assertEqual("testing note", CollectionNote.objects.get(collection=compiled_collection).note)
