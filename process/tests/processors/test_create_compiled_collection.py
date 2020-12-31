from django.db import transaction
from django.test import TransactionTestCase

from process.exceptions import AlreadyExists
from process.models import Collection
from process.processors.compiler import create_compiled_collection


class CreatedCompiledCollectionTests(TransactionTestCase):
    fixtures = ["process/tests/fixtures/complete_db.json"]

    def test_malformed_input(self):
        with self.assertRaises(TypeError) as e:
            create_compiled_collection("")
        self.assertEqual(str(e.exception), "parent_collection_id is not an int value")

    def test_nonexistent_input(self):
        with self.assertRaises(ValueError) as e:
            create_compiled_collection(5)
        self.assertEqual(str(e.exception), "Parent collection (with steps including compile) id 5 not found")

    def test_already_created(self):
        with self.assertRaises(AlreadyExists) as e:
            create_compiled_collection(2)
        self.assertEqual(
            str(e.exception),
            "Compiled collection already created for parent_collection_id 2",
        )

    def test_happy_day(self):
        with transaction.atomic():
            collection = Collection.objects.get(id=3)
            collection.delete()

        collection = create_compiled_collection(2)

        self.assertEqual(collection.parent.id, 2)
        self.assertTrue("compile" in collection.parent.steps)
        self.assertTrue("compile-releases" == collection.transform_type)
