from django.db.utils import IntegrityError
from django.test import TransactionTestCase

from process.models import Collection, CollectionNote
from process.processors.loader import create_master_collection


class CreateMasterCollectionTests(TransactionTestCase):
    fixtures = ["process/tests/fixtures/complete_db.json"]

    def test_malformed_input(self):
        with self.assertRaises(ValueError) as e:
            create_master_collection(
                "test", "wrong_data_version", "testing note", upgrade=True, compile=True, sample=False
            )
        self.assertEqual(
            str(e.exception),
            "data_version 'wrong_data_version' is not in \"YYYY-MM-DD HH:MM:SS\" format or is an invalid date/time",
        )

    def test_integrity_error(self):
        with self.assertRaises(IntegrityError) as e:
            create_master_collection(
                "portugal-releases", "2020-12-29 09:22:08", "testing note", upgrade=False, compile=False, sample=False
            )
        self.assertTrue(str(e.exception).startswith("duplicate key value violates unique constraint "))

    def test_happy_day(self):
        collection, upgraded_collection = create_master_collection(
            "test", "2020-12-29 09:22:08", "testing note", upgrade=True, compile=True, sample=False
        )

        self.assertEqual(upgraded_collection.parent.id, collection.id)
        self.assertTrue("upgrade" in collection.steps)
        self.assertTrue("compile" in upgraded_collection.steps)
        self.assertEqual(Collection.Transforms.UPGRADE_10_11, upgraded_collection.transform_type)
        self.assertEqual("testing note", CollectionNote.objects.get(collection=collection).note)
        self.assertEqual(
            CollectionNote.objects.get(collection=upgraded_collection).note,
            CollectionNote.objects.get(collection=collection).note,
        )
