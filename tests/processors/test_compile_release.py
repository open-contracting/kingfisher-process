from django.test import TransactionTestCase

from process.models import Collection, CollectionNote, CompiledRelease
from process.processors.compiler import compile_release_batch


class CompileReleaseBatchTests(TransactionTestCase):
    fixtures = ["tests/fixtures/complete_db.json"]

    def test_no_releases(self):
        collection = Collection.objects.get(pk=3)
        ocid = "nonexistent"

        result = compile_release_batch(collection, [ocid])

        self.assertEqual(result, [])
        self.assertEqual(CompiledRelease.objects.filter(collection=collection, ocid=ocid).count(), 0)
        self.assertTrue(
            CollectionNote.objects.filter(
                collection=collection, code=CollectionNote.Level.ERROR, note="OCID nonexistent has 0 releases."
            ).exists()
        )

    def test_already_compiled(self):
        collection = Collection.objects.get(pk=3)
        ocid = "ocds-px0z7d-17998-18005-1"

        with self.assertLogs("process.processors.compiler", level="ERROR") as cm:
            result = compile_release_batch(collection, [ocid])

        self.assertEqual(result, [])
        self.assertEqual(CompiledRelease.objects.filter(collection=collection, ocid=ocid).count(), 1)
        self.assertEqual(
            cm.records[0].getMessage(), f"Compiled release {ocid} already exists in collection {collection}"
        )

    def test_happy_day(self):
        collection = Collection.objects.get(pk=3)
        ocid = "ocds-px0z7d-5052-5001-1"
        CompiledRelease.objects.get(collection=collection, ocid=ocid).collection_file.delete()

        result = compile_release_batch(collection, [ocid])

        compiled_release = CompiledRelease.objects.get(collection=collection, ocid=ocid)

        self.assertEqual(result, [ocid])
        self.assertEqual(compiled_release.collection_file.filename, f"{ocid}.json")
