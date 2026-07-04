import json
from collections import OrderedDict
from unittest.mock import Mock, patch

from django.db import IntegrityError, OperationalError
from django.test import SimpleTestCase, TestCase, override_settings
from ocdskit.upgrade import upgrade_10_11
from psycopg import errors

from process.models import CollectionNote, Data
from process.util import create_logger_note, decorator, get_or_create


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


class ErrbackTests(SimpleTestCase):
    def run_errback(self, exception):
        def callback(*args):
            raise exception

        state, channel, method, properties = Mock(), Mock(), Mock(), Mock()
        decorator(lambda *args: {}, callback, state, channel, method, properties, b"{}")
        return state, channel, method

    @patch("process.util.nack")
    @patch("process.util.add_callback_threadsafe")
    def test_foreign_key_violation_shuts_down(self, add_callback_threadsafe, nack):
        exception = IntegrityError("still referenced")
        exception.__cause__ = errors.ForeignKeyViolation("still referenced")

        state, _, _ = self.run_errback(exception)

        add_callback_threadsafe.assert_called_once_with(state.connection, state.interrupt)
        nack.assert_not_called()

    @patch("process.util.time.sleep")
    @patch("process.util.nack")
    @patch("process.util.add_callback_threadsafe")
    def test_deadlock_operational_error_requeues(self, add_callback_threadsafe, nack, sleep):
        exception = OperationalError("deadlock detected")
        exception.__cause__ = errors.DeadlockDetected("deadlock detected")

        state, channel, method = self.run_errback(exception)

        sleep.assert_called_once()
        add_callback_threadsafe.assert_not_called()
        nack.assert_called_once_with(state, channel, method.delivery_tag, requeue=True)

    @patch("process.util.nack")
    @patch("process.util.add_callback_threadsafe")
    def test_other_operational_error_shuts_down(self, add_callback_threadsafe, nack):
        exception = OperationalError("connection lost")

        state, _, _ = self.run_errback(exception)

        add_callback_threadsafe.assert_called_once_with(state.connection, state.interrupt)
        nack.assert_not_called()

    @patch("process.util.nack")
    @patch("process.util.add_callback_threadsafe")
    def test_other_integrity_error_nacks(self, add_callback_threadsafe, nack):
        exception = IntegrityError("duplicate key")
        exception.__cause__ = errors.UniqueViolation("duplicate key")

        state, channel, method = self.run_errback(exception)

        add_callback_threadsafe.assert_not_called()
        nack.assert_called_once_with(state, channel, method.delivery_tag, requeue=False)


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
