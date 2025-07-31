from unittest.mock import patch

from django.core.management import call_command
from django.core.management.base import CommandError
from django.test import TransactionTestCase

from tests.fixtures import collection


class WipeTests(TransactionTestCase):
    def test_missing_args(self):
        with self.assertRaises(CommandError) as e:
            call_command("deletecollection")

        message = "Error: the following arguments are required: collection_id"
        self.assertEqual(str(e.exception), message)

    def test_wrong_args(self):
        with self.assertRaises(CommandError) as e:
            call_command("deletecollection", "text")

        message = "Error: argument collection_id: invalid int value: 'text'"
        self.assertEqual(str(e.exception), message)

    @patch("builtins.input", return_value="y")
    def test_ok(self, mocked):
        source = collection()
        source.save()
        call_command("deletecollection", source.id)  # no error
