import datetime
from unittest.mock import call, patch

from django.core.management import call_command
from django.core.management.base import CommandError
from django.test import TransactionTestCase

from process.models import CollectionFile
from process.tests.fixtures import collection


class AddstepTests(TransactionTestCase):
    def test_exists(self):
        with self.assertRaises(CommandError) as e:
            call_command("addstep", "0", "compile-releases")

        self.assertEqual(str(e.exception), "Collection 0 does not exist")

    # The following tests mirror those in `test_models.py` for `Collection.add_step()` and `Collection.clean_fields()`.

    @patch("process.management.commands.base.worker.BaseWorker.publish")
    def test_check(self, publish):
        source = collection()
        source.save()

        pks = []
        for filename in map(str, range(1, 4)):
            collection_file = CollectionFile(collection=source, filename=filename)
            collection_file.save()
            pks.append(collection_file.pk)

        call_command("addstep", source.pk, "check")

        source.refresh_from_db()
        calls = [call("check", {"file_id": pk, "source_id": source.pk}) for pk in pks]

        self.assertTrue(source.steps["check"])
        publish.assert_has_calls(calls)

    @patch("process.management.commands.base.worker.BaseWorker.publish")
    def test_transform(self, publish):
        source = collection()
        source.save()

        pks = []
        for filename in map(str, range(1, 4)):
            collection_file = CollectionFile(collection=source, filename=filename)
            collection_file.save()
            pks.append(collection_file.pk)

        for transform_type in ("compile-releases", "upgrade-1-0-to-1-1"):
            with self.subTest(transform_type=transform_type):
                call_command("addstep", source.pk, transform_type)

                source.refresh_from_db()
                transforms = source.collection_set.filter(transform_type=transform_type)

                self.assertTrue(source.steps[transform_type])
                self.assertEqual(len(transforms), 1)

                destination = transforms[0]
                calls = [
                    call(transform_type, {"file_id": pk, "source_id": source.pk, "destination_id": destination.pk})
                    for pk in pks
                ]

                self.assertEqual(destination.source_id, "france")
                self.assertEqual(destination.data_version, datetime.datetime(2001, 1, 1, 0, 0))
                self.assertFalse(destination.sample)
                self.assertEqual(destination.expected_files_count, None)
                self.assertEqual(destination.parent_id, source.pk)
                self.assertEqual(destination.transform_type, transform_type)
                publish.assert_has_calls(calls)

    def test_deleted_at(self):
        source = collection(deleted_at="2001-01-01 00:00:00")
        source.save()

        with self.assertRaises(CommandError) as e:
            call_command("addstep", source.pk, "compile-releases")

        source.refresh_from_db()

        self.assertEqual(str(e.exception), "Parent collection {} is being deleted".format(source.pk))
        self.assertNotIn("compile-releases", source.steps)

    def test_double_transform(self):
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

                with self.assertRaises(CommandError) as e:
                    call_command("addstep", original.pk, transform_type)

                original.refresh_from_db()

                self.assertEqual(str(e.exception), message.format(original.pk, original.parent_id))
                self.assertNotIn(transform_type, original.steps)

    def test_disallowed_transition(self):
        source = collection()
        source.save()

        compiled = collection(parent=source, transform_type="compile-releases")
        compiled.save()

        with self.assertRaises(CommandError) as e:
            call_command("addstep", compiled.pk, "upgrade-1-0-to-1-1")

        compiled.refresh_from_db()
        message = "Parent collection {} is compiled and can't be upgraded"

        self.assertEqual(str(e.exception), message.format(compiled.pk))
        self.assertNotIn("upgrade-1-0-to-1-1", compiled.steps)

    def test_duplicate(self):
        source = collection()
        source.save()

        destination = collection(parent=source, transform_type="compile-releases")
        destination.save()

        with self.assertRaises(CommandError) as e:
            call_command("addstep", source.pk, "compile-releases")

        destination.refresh_from_db()
        message = "Parent collection {} is already transformed into {}\nCollection with this Parent and Transform type already exists."  # noqa: E501

        self.assertEqual(str(e.exception), message.format(source.pk, destination.pk))
        self.assertNotIn("compile-releases", destination.steps)
