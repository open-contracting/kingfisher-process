from unittest.mock import MagicMock, patch

from django.test import TransactionTestCase
from django.utils import timezone

from process.management.commands.api_loader import callback
from tests.fixtures import collection


class ApiLoaderCallbackTests(TransactionTestCase):
    def setUp(self):
        self.collection = collection()
        self.collection.save()

    def _callback(self, client_state, channel, collection_id):
        method = MagicMock(delivery_tag=1)
        message = {"collection_id": collection_id, "url": "http://example.com/x.json", "path": "x.json"}
        callback(client_state, channel, method, MagicMock(), message)

    @patch("process.management.commands.api_loader.publish")
    @patch("process.management.commands.api_loader.ack")
    def test_acks_when_collection_deleted(self, ack, publish):
        client_state, channel = MagicMock(), MagicMock()
        self._callback(client_state, channel, self.collection.pk + 1000)  # as if fully deleted

        ack.assert_called_once_with(client_state, channel, 1)
        publish.assert_not_called()

    @patch("process.management.commands.api_loader.publish")
    @patch("process.management.commands.api_loader.ack")
    def test_acks_when_collection_cancelled(self, ack, publish):
        self.collection.deleted_at = timezone.now()
        self.collection.save()

        client_state, channel = MagicMock(), MagicMock()
        self._callback(client_state, channel, self.collection.pk)

        ack.assert_called_once_with(client_state, channel, 1)
        publish.assert_not_called()
