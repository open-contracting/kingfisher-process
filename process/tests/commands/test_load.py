from django.core.management import call_command
from django.core.management.base import CommandError
from django.test import TransactionTestCase

from process.tests.fixtures import collection


class LoadTests(TransactionTestCase):
    def test_deleted_at(self):
        source = collection(deleted_at='2001-01-01 00:00:00')
        source.save()

        with self.assertRaises(CommandError) as e:
            call_command('load', '--collection', source.pk, 'file.json')

        self.assertEqual(str(e.exception), 'Collection {} is being deleted'.format(source.pk))

    def test_store_end_at(self):
        source = collection(store_end_at='2001-01-01 00:00:00')
        source.save()

        with self.assertRaises(CommandError) as e:
            call_command('load', '--collection', source.pk, 'file.json')

        self.assertIn('Collection {} is closed to new files'.format(source.pk), str(e.exception))
