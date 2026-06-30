from unittest.mock import patch

from django.core.management import call_command
from django.test import TransactionTestCase, override_settings

from process.models import Collection, CollectionFile, CompiledRelease, ProcessingStep, Release


@patch("process.management.commands.compilebacklog.get_publisher")
class CompileBacklogTests(TransactionTestCase):
    fixtures = ["tests/fixtures/complete_db.json"]

    def run_backlog(self):
        collection = Collection.objects.get(pk=3)
        # Remove existing compiled releases (cascades from their collection files) so the OCIDs can be recompiled.
        CollectionFile.objects.filter(collection=collection).delete()

        ocids = list(
            Release.objects.filter(collection_id=collection.parent_id).values_list("ocid", flat=True).distinct()
        )
        ProcessingStep.objects.bulk_create(
            [ProcessingStep(name=ProcessingStep.Name.COMPILE, collection=collection, ocid=ocid) for ocid in ocids]
        )

        call_command("compilebacklog", "--batch-size", "30")

        return collection, ocids

    def test_deduplicate(self, get_publisher):
        collection, ocids = self.run_backlog()

        self.assertEqual(ProcessingStep.objects.filter(name=ProcessingStep.Name.COMPILE).count(), 0)
        self.assertEqual(
            set(CompiledRelease.objects.filter(collection=collection).values_list("ocid", flat=True)), set(ocids)
        )

    @override_settings(DEDUPLICATE_DATA=False)
    def test_without_deduplicate(self, get_publisher):
        collection, ocids = self.run_backlog()

        self.assertEqual(ProcessingStep.objects.filter(name=ProcessingStep.Name.COMPILE).count(), 0)
        self.assertEqual(CompiledRelease.objects.filter(collection=collection).count(), len(ocids))
