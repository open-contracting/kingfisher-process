import json
from collections import OrderedDict
from unittest.mock import patch

from django.test import SimpleTestCase
from ocdskit.upgrade import upgrade_10_11

from process.models import CollectionNote
from process.util import create_logger_note


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
            'party in "supplier" role differs from party in ["tenderer"] roles:\n{"name": "Acme Inc.", "identifier": '
            '{"id": 1}, "additionalIdentifiers": [{"id": "a"}], "id": "3c9756cf8983b14066a034079aa7aae4"}\n{"id": '
            '"3c9756cf8983b14066a034079aa7aae4", "name": "Acme Inc.", "identifier": {"id": 1}}\n',
        )

    @patch("process.util.create_note")
    def test_create_logger_note_not_called(self, create_note):
        with create_logger_note("collection", "ocdskit"):
            upgrade_10_11({})

        create_note.assert_not_called()
