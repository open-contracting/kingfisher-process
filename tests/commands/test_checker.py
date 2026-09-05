from unittest.mock import MagicMock, patch

from django.test import TransactionTestCase

from process.management.commands.checker import callback


class CheckerCallbackTests(TransactionTestCase):
    @patch("process.management.commands.checker.publish")
    @patch("process.management.commands.checker.ack")
    def test_acks_when_collection_file_deleted(self, ack, publish):
        client_state, channel = MagicMock(), MagicMock()
        method = MagicMock(delivery_tag=1)
        message = {"collection_id": 10000, "collection_file_id": 10000}

        callback(client_state, channel, method, MagicMock(), message)

        ack.assert_called_once_with(client_state, channel, 1)
        publish.assert_not_called()
