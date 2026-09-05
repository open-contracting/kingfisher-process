from unittest.mock import MagicMock, patch

from django.conf import settings
from django.test import TransactionTestCase
from django.utils import timezone

from process.management.commands.compiler import callback
from process.models import Collection, CollectionFile, CompiledRelease, Data, PackageData, ProcessingStep, Release


class CompilerCallbackTests(TransactionTestCase):
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
        )

        collection_file = CollectionFile.objects.create(collection=self.parent, filename="data.json")
        package_data = PackageData.objects.create(hash_md5="", data={})
        data = Data.objects.create(hash_md5="", data={})

        for ocid in ("ocds-a", "ocds-b"):
            Release.objects.create(
                collection=self.parent,
                collection_file=collection_file,
                ocid=ocid,
                data=data,
                package_data=package_data,
            )

    def _method(self, *, redelivered):
        return MagicMock(
            routing_key=f"{settings.RABBIT_EXCHANGE_NAME}_collection_closed", redelivered=redelivered, delivery_tag=1
        )

    def _call(self, *, redelivered):
        callback(
            MagicMock(),
            MagicMock(),
            self._method(redelivered=redelivered),
            MagicMock(),
            {"collection_id": self.parent.pk},
        )

    def _compile_step_ocids(self):
        return set(
            ProcessingStep.objects.filter(collection=self.compiled, name=ProcessingStep.Name.COMPILE).values_list(
                "ocid", flat=True
            )
        )

    @patch("process.management.commands.compiler.publish")
    @patch("process.management.commands.compiler.ack")
    def test_acks_when_collection_deleted(self, ack, publish):
        client_state, channel = MagicMock(), MagicMock()
        method = self._method(redelivered=False)
        message = {"collection_id": self.parent.pk + 1000}  # as if fully deleted

        callback(client_state, channel, method, MagicMock(), message)

        ack.assert_called_once_with(client_state, channel, 1)
        publish.assert_not_called()

    @patch("process.management.commands.compiler.publish")
    @patch("process.management.commands.compiler.ack")
    def test_acks_when_collection_file_deleted(self, ack, publish):
        client_state, channel = MagicMock(), MagicMock()
        method = MagicMock(routing_key=f"{settings.RABBIT_EXCHANGE_NAME}_file_worker", delivery_tag=1)
        message = {"collection_id": self.parent.pk, "collection_file_id": 10000}

        callback(client_state, channel, method, MagicMock(), message)

        ack.assert_called_once_with(client_state, channel, 1)
        publish.assert_not_called()

    @patch("process.management.commands.compiler.publish")
    @patch("process.management.commands.compiler.ack")
    def test_happy_day(self, ack, publish):
        self._call(redelivered=False)

        self.compiled.refresh_from_db()
        self.assertTrue(self.compiled.compilation_enqueued)
        self.assertEqual(self._compile_step_ocids(), {"ocds-a", "ocds-b"})
        publish.assert_called_once()
        ack.assert_called_once()

    @patch("process.management.commands.compiler.publish")
    @patch("process.management.commands.compiler.ack")
    def test_compilation_started_message_not_redelivered(self, ack, publish):
        self.compiled.compilation_started = True
        self.compiled.save()

        self._call(redelivered=False)

        self.compiled.refresh_from_db()
        self.assertFalse(self.compiled.compilation_enqueued)
        self.assertEqual(self._compile_step_ocids(), set())
        publish.assert_not_called()
        ack.assert_called_once()

    @patch("process.management.commands.compiler.publish")
    @patch("process.management.commands.compiler.ack")
    def test_compilation_started_message_redelivered(self, ack, publish):
        self.compiled.compilation_started = True
        self.compiled.save()

        self._call(redelivered=True)

        self.compiled.refresh_from_db()
        self.assertTrue(self.compiled.compilation_enqueued)
        self.assertEqual(self._compile_step_ocids(), {"ocds-a", "ocds-b"})
        publish.assert_called_once()
        ack.assert_called_once()

    @patch("process.management.commands.compiler.publish")
    @patch("process.management.commands.compiler.ack")
    def test_compilation_started_message_redelivered_republish_all(self, ack, publish):
        self.compiled.compilation_started = True
        self.compiled.save()

        collection_file = CollectionFile.objects.create(collection=self.compiled, filename="ocds-a.json")
        CompiledRelease.objects.create(
            collection=self.compiled,
            collection_file=collection_file,
            data=Data.objects.create(hash_md5="", data={}),
            ocid="ocds-a",
        )

        self._call(redelivered=True)

        self.assertEqual(self._compile_step_ocids(), {"ocds-a", "ocds-b"})
