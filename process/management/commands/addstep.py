from django.core.exceptions import ValidationError
from django.core.management.base import CommandError
from django.db import transaction
from django.utils.translation import gettext
from django.utils.translation import gettext_lazy as _

from process.management.commands.base.collection_command import CollectionCommand
from process.models import Collection


class Command(CollectionCommand):
    help = gettext("Adds a step to the collection's processing pipeline")

    worker_name = "add_step"

    def __init__(self):
        super().__init__(self.worker_name)

    def add_collection_arguments(self, parser):
        parser.add_argument('step', choices=['check'] + Collection.Transforms.values, help=_('the step to add'))

    def handle_collection(self, collection, *args, **options):
        step = options['step']

        # This command updates the source collection's configuration, so that any newly loaded data is transformed.
        #
        # It also needs to enqueue for transformation any data already loaded into the source collection. If data is
        # still loading into the source collection, we need to avoid two race conditions:
        #
        # 1. If we were to first select the data to enqueue, and then update the collection's configuration, and new
        #    data were loaded in between, then that data would never be enqueued.
        # 2. If we were to first update the collection's configuration, and then select the data to enqueue, and new
        #    data were loaded in between, then that data would be enqueued twice.
        #
        # To be clear, a race condition occurs only if this command and a loader run concurrently, not serially.
        #
        # The solution is to make two transactions commit in a predictable order, by locking the collection's row:
        #
        # 1. This command UPDATEs the collection's configuration then SELECTs the data, in one transaction.
        # 2. A loader INSERTs the data then SELECTs FOR SHARE the collection's configuration, in one transaction.
        #
        # In PostgreSQL transactions, SELECT queries can read committed data, but not uncommitted data. SELECT FOR
        # SHARE blocks (and is blocked by) UPDATE, but doesn't block another SELECT FOR SHARE.
        #
        # https://www.postgresql.org/docs/current/transaction-iso.html#XACT-READ-COMMITTED
        # https://www.postgresql.org/docs/current/explicit-locking.html#LOCKING-ROWS
        #
        # By using SELECT FOR SHARE, loaders don't block each other. (If SELECT FOR UPDATE were used, they would.)
        #
        # If this command UPDATEs before a loader SELECTs FOR SHARE, the loader will wait for this command to commit.
        # This command won't have seen the loader's new data, but the loader will then see the new configuration, and
        # therefore enqueue its new data.
        #
        # If a loader SELECTs FOR SHARE before this command UPDATEs, this command will wait for the loader to commit.
        # The loader won't have seen this command's new configuration, but this command will then see the new data,
        # and therefore enqueue it.
        #
        # All scenarios can be tested by opening two PostgreSQL shells and running, in each, one line at a time, in any
        # order you choose (substituting % for an existing collection's ID):
        #
        # BEGIN;
        # UPDATE collection SET check_data = true WHERE id = %;
        # SELECT * FROM collection_file;
        # COMMIT;
        #
        # BEGIN;
        # INSERT INTO collection_file (collection_id, filename, url) VALUES (%, 'test', '');
        # SELECT check_data FROM collection WHERE id = % FOR SHARE;
        # COMMIT;
        #
        # Note: The queries above use the now-deprecated `check_data` field.
        with transaction.atomic():
            try:
                destination = collection.add_step(step)
            except ValidationError as e:
                raise CommandError('\n'.join(e.messages))

            message = {'source_id': collection.pk}
            if destination:
                message['destination_id'] = destination.pk

            file_ids = collection.collectionfile_set.values_list('id', flat=True)
            for file_id in file_ids.iterator():
                self.publish(step, dict(file_id=file_id, **message))
