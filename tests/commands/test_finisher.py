from unittest.mock import MagicMock, patch

from django.test import TransactionTestCase
from django.utils import timezone

from process.management.commands.finisher import callback
from process.models import Collection


class FinisherCallbackTests(TransactionTestCase):
    def setUp(self):
        self.parent = Collection.objects.create(
            source_id="test_release_package",
            data_version="2023-01-01T00:00:00Z",
            data_type={"format": "release package", "array": False, "concatenated": False},
            store_end_at=timezone.now(),
            steps=["compile"],
        )
        self.compiled = Collection.objects.create(
            source_id="test_release_package",
            data_version="2023-01-01T00:00:00Z",
            parent=self.parent,
            transform_type=Collection.Transform.COMPILE_RELEASES,
            compilation_started=True,
        )

    def _callback(self, client_state, channel):
        method = MagicMock(redelivered=True, delivery_tag=1)
        callback(client_state, channel, method, MagicMock(), {"collection_id": self.compiled.pk})

    @patch("process.management.commands.finisher.time.sleep")
    @patch("process.management.commands.finisher.nack")
    @patch("process.management.commands.finisher.ack")
    def test_requeues_until_compilation_enqueued(self, ack, nack, sleep):
        client_state, channel = MagicMock(), MagicMock()

        self._callback(client_state, channel)

        self.compiled.refresh_from_db()
        self.assertIsNone(self.compiled.completed_at)
        nack.assert_called_once_with(client_state, channel, 1, requeue=True)
        ack.assert_not_called()

    @patch("process.management.commands.finisher.nack")
    @patch("process.management.commands.finisher.ack")
    def test_completes_once_compilation_enqueued(self, ack, nack):
        self.compiled.compilation_enqueued = True
        self.compiled.save()
        client_state, channel = MagicMock(), MagicMock()

        self._callback(client_state, channel)

        self.compiled.refresh_from_db()
        self.assertIsNotNone(self.compiled.completed_at)
        nack.assert_not_called()
        ack.assert_called_once()
