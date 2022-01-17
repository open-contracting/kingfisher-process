from unittest.mock import patch

from django.core.management import call_command
from django.core.management.base import CommandError
from django.test import TransactionTestCase

from process.models import Collection
from tests.fixtures import collection


class WipeTests(TransactionTestCase):
    def test_missing_args(self):
        with self.assertRaises(CommandError) as e:
            call_command("close")

        message = "Please indicate collection by its id."
        self.assertEqual(str(e.exception), message)

    def test_wrong_args(self):
        with self.assertRaises(CommandError) as e:
            call_command("close", "-c", "dfds")

        message = "--collection dfds is not an int value"
        self.assertEqual(str(e.exception), message)

    @patch("builtins.input", return_value="Y")
    def test_ok(self, mocked):
        source = collection()
        source.save()
        call_command("wipe_collection", "-c", source.id)
        with self.assertRaises(Collection.DoesNotExist):
            source.refresh_from_db()
