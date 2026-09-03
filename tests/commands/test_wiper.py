import threading
import time
from unittest.mock import patch

from django.db import OperationalError, connection, transaction
from django.test import TransactionTestCase, override_settings

from process.management.commands.wiper import delete_collection
from process.models import (
    Collection,
    CollectionFile,
    CollectionNote,
    CompiledRelease,
    Data,
    PackageData,
    Release,
)
from process.util import lock_collection
from tests.fixtures import collection


class WiperTests(TransactionTestCase):
    def build_collection(self):
        source = collection()
        source.save()

        collection_file = CollectionFile(collection=source, filename="x.json")
        collection_file.save()

        release_data = Data.objects.create(hash_md5="", data={"ocid": "ocds-1", "tag": ["tender"]})
        compiled_data = Data.objects.create(hash_md5="", data={"ocid": "ocds-1", "tag": ["compiled"]})
        package_data = PackageData.objects.create(hash_md5="", data={"version": "1.1"})

        Release.objects.create(
            collection=source,
            collection_file=collection_file,
            ocid="ocds-1",
            data=release_data,
            package_data=package_data,
        )
        CompiledRelease.objects.create(
            collection=source,
            collection_file=collection_file,
            ocid="ocds-1",
            data=compiled_data,
        )

        return source

    @override_settings(DEDUPLICATE_DATA=False)
    def test_deletes_data_if_no_deduplicate(self):
        source = self.build_collection()

        delete_collection(source.id)

        self.assertEqual(Collection.objects.count(), 0)
        self.assertEqual(Release.objects.count(), 0)
        self.assertEqual(CompiledRelease.objects.count(), 0)
        self.assertEqual(Data.objects.count(), 0)
        self.assertEqual(PackageData.objects.count(), 0)

    @override_settings(DEDUPLICATE_DATA=True)
    def test_keeps_data_if_deduplicate(self):
        source = self.build_collection()

        delete_collection(source.id)

        self.assertEqual(Collection.objects.count(), 0)
        self.assertEqual(Release.objects.count(), 0)
        self.assertEqual(CompiledRelease.objects.count(), 0)
        self.assertEqual(Data.objects.count(), 2)
        self.assertEqual(PackageData.objects.count(), 1)

    @override_settings(DEDUPLICATE_DATA=False)
    def test_rolls_back_on_error(self):
        source = self.build_collection()

        # Simulate a deadlock (or any failure) after the raw-SQL DELETEs. The deletion must roll back entirely, so that
        # a requeued retry can re-derive the package_data and data ids and delete them, instead of orphaning them.
        with patch("process.management.commands.wiper.Collection.objects") as manager:
            manager.filter.return_value.delete.side_effect = OperationalError("deadlock detected")
            with self.assertRaises(OperationalError):
                delete_collection(source.id)

        self.assertEqual(Collection.objects.count(), 1)
        self.assertEqual(Release.objects.count(), 1)
        self.assertEqual(CompiledRelease.objects.count(), 1)
        self.assertEqual(Data.objects.count(), 2)
        self.assertEqual(PackageData.objects.count(), 1)


class LockTests(TransactionTestCase):
    def test_deletion_waits_for_locked_collection(self):
        source = collection()
        source.save()

        locked = threading.Event()
        errors = []

        def worker():
            try:
                with transaction.atomic():
                    self.assertIsNotNone(lock_collection(source.pk))
                    locked.set()

                    # Give delete_collection() time to run its DELETEs, if it isn't blocked by the lock.
                    time.sleep(1)

                    CollectionNote(collection=source, code=CollectionNote.Level.INFO, note="x").save()
            except Exception as e:  # noqa: BLE001
                errors.append(e)
            finally:
                connection.close()

        thread = threading.Thread(target=worker)
        thread.start()

        self.assertTrue(locked.wait(10))

        delete_collection(source.pk)

        thread.join()

        self.assertEqual(errors, [])
        self.assertEqual(Collection.objects.count(), 0)
        self.assertEqual(CollectionNote.objects.count(), 0)

    def test_lock_returns_none_if_deleted(self):
        source = collection()
        source.save()

        deleting = threading.Event()
        collections = []

        def worker():
            try:
                self.assertTrue(deleting.wait(10))
                with transaction.atomic():
                    collections.append(lock_collection(source.pk))
            finally:
                connection.close()

        thread = threading.Thread(target=worker)
        thread.start()

        with transaction.atomic():
            delete_collection(source.pk)
            deleting.set()

            # Give lock_collection() time to block on the lock, before committing the deletion.
            time.sleep(1)

        thread.join()

        self.assertEqual(collections, [None])
