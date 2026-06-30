import time

from django.conf import settings
from django.core.management.base import BaseCommand
from django.db import transaction
from django.utils.translation import gettext as t
from django.utils.translation import gettext_lazy as _
from ocdskit.util import Format

from process.models import Collection, ProcessingStep
from process.processors.compiler import compile_release_batch
from process.util import get_publisher
from process.util import wrap as w

# The routing key that release_compiler publishes, consumed by the finisher to complete the compiled collection.
routing_key = "release_compiler"


class Command(BaseCommand):
    help = w(
        t(
            "Compile the OCIDs that have a pending COMPILE step, in batches, without using the message queue. Use "
            "this to drain a backlog of release_compiler messages: it compiles many OCIDs per transaction, instead "
            "of one OCID per message."
        )
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--batch-size",
            type=int,
            default=settings.COMPILE_BATCH_SIZE,
            help=_("the number of OCIDs to compile per transaction"),
        )

    def handle(self, *args, **options):
        batch_size = options["batch_size"]

        collection_ids = list(
            ProcessingStep.objects.filter(name=ProcessingStep.Name.COMPILE)
            .values_list("collection_id", flat=True)
            .distinct()
        )

        start = time.monotonic()
        total = 0

        with get_publisher() as client:
            for collection_id in collection_ids:
                collection = Collection.objects.select_related("parent").get(pk=collection_id)

                # Record packages are compiled by the record_compiler worker; this command drains release compilations.
                if not collection.parent or collection.parent.data_type.get("format") != Format.release_package:
                    continue

                steps = ProcessingStep.objects.filter(name=ProcessingStep.Name.COMPILE, collection_id=collection_id)
                while ocids := list(steps.values_list("ocid", flat=True)[:batch_size]):
                    with transaction.atomic():
                        compile_release_batch(collection, ocids)
                        # Delete every step in the batch, including for OCIDs that were skipped (already compiled, or
                        # with no releases), to match the per-message behavior and to make progress.
                        steps.filter(ocid__in=ocids).delete()

                    total += len(ocids)
                    elapsed = time.monotonic() - start
                    rate = total / elapsed if elapsed else 0
                    # Write without a newline and flush, so the line updates live (stderr is line-buffered).
                    self.stderr.write(f"\r{total} OCIDs ({rate:.0f}/s)", ending="")
                    self.stderr.flush()

                # The command bypasses the queue, so trigger the finisher to complete the (now step-less) collection.
                client.publish({"collection_id": collection.pk}, routing_key=routing_key)

        elapsed = time.monotonic() - start
        rate = total / elapsed if elapsed else 0
        self.stderr.write(self.style.SUCCESS(f"\ndone: {total} OCIDs in {elapsed:.0f}s ({rate:.0f}/s)"))
