from django.db import transaction
from django.test import TransactionTestCase

from process.models import CompiledRelease
from process.processors.compiler import compile_release


class CompilerTests(TransactionTestCase):
    fixtures = ["fixtures.json"]

    def test_malformed_input(self):
        with self.assertRaises(TypeError) as e:
            compile_release("", "")
        self.assertEqual(str(e.exception), "collection_id is not an int value")

        with self.assertRaises(TypeError) as e:
            compile_release(1, 1)
        self.assertEqual(str(e.exception), "ocid is not a string value")

    def test_nonexistent_input(self):
        with self.assertRaises(ValueError) as e:
            compile_release(4, "")
        self.assertEqual(str(e.exception), "Compiled collection with parent collection id 4 not found")

        with self.assertRaises(ValueError) as e:
            compile_release(2, "sdf")
        self.assertEqual(str(e.exception), "No releases with ocid sdf found in parent collection.")

    def test_already_compiled(self):
        with self.assertRaises(ValueError) as e:
            compile_release(2, "ocds-px0z7d-17998-18005-1")
        self.assertEqual(
            str(e.exception),
            "CompiledRelease ocds-px0z7d-17998-18005-1 (id: 45) for a collection portugal-releases:2020-12-29 09:22:08 (id: 3) already exists",  # noqa: E501
        )

    def test_happy_day(self):
        ocid = "ocds-px0z7d-5052-5001-1"
        with transaction.atomic():
            compiled_release = CompiledRelease.objects.filter(ocid__exact=ocid).get(
                collection_file_item__collection_file__collection__id=3
            )
            compiled_release.collection_file_item.collection_file.delete()
        release = compile_release(2, ocid)

        self.assertEqual(release.ocid, ocid)
        self.assertEqual(release.collection.id, 3)
        self.assertEqual(release.collection_file_item.number, 0)
        self.assertEqual(release.collection_file_item.collection_file.filename, "{}.json".format(ocid))
