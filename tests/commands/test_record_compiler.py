from unittest.mock import MagicMock, patch

from django.test import TransactionTestCase
from django.utils import timezone

from process.management.commands.record_compiler import callback
from process.models import Collection


class RecordCompilerCallbackTests(TransactionTestCase):
    def setUp(self):
        self.parent = Collection.objects.create(
            source_id="test_record_package",
            data_version="2023-01-01T00:00:00Z",
            data_type={"format": "record package", "array": False, "concatenated": False},
            store_end_at=timezone.now(),
            steps=["compile"],
        )
        self.compiled = Collection.objects.create(
            source_id="test_record_package",
            data_version="2023-01-01T00:00:00Z",
            parent=self.parent,
            transform_type=Collection.Transform.COMPILE_RELEASES,
            compilation_started=True,
        )

    def _callback(self, client_state, channel, compiled_collection_id):
        method = MagicMock(delivery_tag=1)
        message = {"collection_id": self.parent.pk, "compiled_collection_id": compiled_collection_id, "ocid": "a"}
        callback(client_state, channel, method, MagicMock(), message)

    @patch("process.management.commands.record_compiler.publish")
    @patch("process.management.commands.record_compiler.ack")
    def test_acks_when_collection_deleted(self, ack, publish):
        client_state, channel = MagicMock(), MagicMock()
        self._callback(client_state, channel, self.compiled.pk + 1000)  # as if fully deleted

        ack.assert_called_once_with(client_state, channel, 1)
        publish.assert_not_called()

    @patch("process.management.commands.record_compiler.publish")
    @patch("process.management.commands.record_compiler.ack")
    def test_acks_when_collection_cancelled(self, ack, publish):
        self.compiled.deleted_at = timezone.now()
        self.compiled.save()

        client_state, channel = MagicMock(), MagicMock()
        self._callback(client_state, channel, self.compiled.pk)

        ack.assert_called_once_with(client_state, channel, 1)
        publish.assert_not_called()
