from unittest.mock import patch

from django.test import TransactionTestCase

from process.exceptions import AlreadyExists
from process.management.commands.release_compiler import compile_release
from process.models import Collection, CollectionNote, CompiledRelease


class CompileReleaseTests(TransactionTestCase):
    fixtures = ["tests/fixtures/complete_db.json"]

    @patch("process.management.commands.release_compiler.create_note")
    def test_nonexistent_input(self, create_note):
        release = compile_release(3, "nonexistent")

        self.assertIsNone(release)
        create_note.assert_called_once_with(
            Collection.objects.get(pk=3),
            CollectionNote.Level.ERROR,
            "OCID nonexistent has 0 releases.",
        )

    def test_already_compiled(self):
        with self.assertRaises(AlreadyExists) as e:
            compile_release(3, "ocds-px0z7d-17998-18005-1")
        self.assertEqual(
            str(e.exception),
            "Compiled release ocds-px0z7d-17998-18005-1 (id: 45) already exists in collection "
            "portugal_releases:2020-12-29 09:22:08 (id: 3)",
        )

    def test_happy_day(self):
        ocid = "ocds-px0z7d-5052-5001-1"
        compiled_release = CompiledRelease.objects.get(collection_id=3, ocid=ocid)
        compiled_release.collection_file_item.collection_file.delete()
        release = compile_release(3, ocid)

        self.assertEqual(release.ocid, ocid)
        self.assertEqual(release.collection.id, 3)
        self.assertEqual(release.collection_file_item.number, 0)
        self.assertEqual(release.collection_file_item.collection_file.filename, f"{ocid}.json")
