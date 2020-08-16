import os.path
from unittest.mock import patch

from django.core.management import call_command
from django.core.management.base import CommandError
from django.test import TransactionTestCase
from django.test.utils import captured_stderr

from process.tests.fixtures import collection


def path(filename):
    return os.path.join('process', 'tests', 'fixtures', filename)


class LoadTests(TransactionTestCase):
    def test_missing_args(self):
        with self.assertRaises(CommandError) as e:
            call_command('load', path('file.json'))

        message = 'Please indicate either a new collection (using --source and --note and, optionally, --time and ' \
                  '--sample) or an open collection (using --collection)'
        self.assertEqual(str(e.exception), message)

    def test_missing_note(self):
        with self.assertRaises(CommandError) as e:
            call_command('load', '--source', 'france', path('file.json'))

        self.assertEqual(str(e.exception), 'You must add a note (using --note) when loading into a new collection')

    def test_mixed_args(self):
        source = collection()
        source.save()

        for args in (['--source', 'france'], ['--time', '2001-01-01 00:00:00'], ['--sample']):
            with self.subTest(args=args):
                with self.assertRaises(CommandError) as e:
                    call_command('load', '--collection', source.pk, *args, path('file.json'))

                self.assertEqual(str(e.exception), 'You cannot mix options for a new collection (--source, --time, '
                                                   '--sample) and for an open collection (--collection)')

    def test_collection_type(self):
        with self.assertRaises(CommandError) as e:
            call_command('load', '--collection', 'nonexistent', path('file.json'))

        self.assertEqual(str(e.exception), "Error: argument --collection: invalid int value: 'nonexistent'")

    def test_collection_nonexistent(self):
        with self.assertRaises(CommandError) as e:
            call_command('load', '--collection', '999', path('file.json'))

        self.assertEqual(str(e.exception), 'Collection 999 does not exist')

    def test_collection_deleted_at(self):
        source = collection(deleted_at='2001-01-01 00:00:00')
        source.save()

        with self.assertRaises(CommandError) as e:
            call_command('load', '--collection', source.pk, path('file.json'))

        self.assertEqual(str(e.exception), 'Collection {} is being deleted'.format(source.pk))

    def test_collection_store_end_at(self):
        source = collection(store_end_at='2001-01-01 00:00:00')
        source.save()

        with self.assertRaises(CommandError) as e:
            call_command('load', '--collection', source.pk, path('file.json'))

        self.assertIn('Collection {} is closed to new files'.format(source.pk), str(e.exception))

    def test_path_nonexistent(self):
        with self.assertRaises(CommandError) as e:
            call_command('load', '--source', 'france', '--note', 'x', 'nonexistent.json')

        self.assertEqual(str(e.exception), "Error: argument PATH: No such file or directory 'nonexistent.json'")

    def test_path_empty(self):
        with self.assertRaises(CommandError) as e:
            call_command('load', '--source', 'france', '--note', 'x', path('empty'))

        self.assertEqual(str(e.exception), 'No files found')

    def test_time_future(self):
        with self.assertRaises(CommandError) as e:
            call_command('load', '--source', 'france', '--note', 'x', '--time', '3000-01-01 00:00', path('file.json'))

        message = "'3000-01-01 00:00' is greater than the earliest file modification time: '20"
        self.assertTrue(str(e.exception).startswith(message))

    def test_time_invalid(self):
        for value in ('2000-01-01 00:', '2000-01-01 24:00:00'):
            with self.subTest(value=value):
                with self.assertRaises(CommandError) as e:
                    call_command('load', '--source', 'france', '--note', 'x', '--time', value, path('file.json'))

                self.assertEqual(str(e.exception), 'data_version \'{}\' is not in "YYYY-MM-DD HH:MM:SS" format or is '
                                                   'an invalid date/time'.format(value))

    def test_source_invalid(self):
        with captured_stderr() as stderr:
            try:
                call_command('load', '--source', 'nonexistent', '--note', 'x', path('file.json'))
            except Exception as e:
                self.fail('Unexpected exception {}'.format(e))

            self.assertTrue("The --source argument can't be validated, because a Scrapyd URL is not configured in "
                            "settings.py." in stderr.getvalue())

    @patch('process.scrapyd.spiders')
    def test_source_invalid_scrapyd(self, spiders):
        spiders.return_value = ['france']

        with captured_stderr() as stderr, self.settings(SCRAPYD={'url': 'http://', 'project': 'kingfisher'}):
            with self.assertRaises(CommandError) as e:
                call_command('load', '--source', 'nonexistent', '--note', 'x', path('file.json'))

            self.assertEqual(str(e.exception), "source_id: 'nonexistent' is not a spider in the kingfisher project "
                                               "of Scrapyd")

            self.assertTrue('Use --force to ignore the following error:' in stderr.getvalue())

    @patch('process.scrapyd.spiders')
    def test_source_invalid_scrapyd_close(self, spiders):
        spiders.return_value = ['france']

        with self.settings(SCRAPYD={'url': 'http://example.com', 'project': 'kingfisher'}):
            with self.assertRaises(CommandError) as e:
                call_command('load', '--source', 'farnce', '--note', 'x', path('file.json'))

            self.assertEqual(str(e.exception), "source_id: 'farnce' is not a spider in the kingfisher project of "
                                               "Scrapyd. Did you mean: france")

    @patch('process.scrapyd.spiders')
    def test_source_invalid_scrapyd_force(self, spiders):
        spiders.return_value = ['france']

        with self.settings(SCRAPYD={'url': 'http://example.com', 'project': 'kingfisher'}):
            try:
                call_command('load', '--source', 'nonexistent', '--note', 'x', '--force', path('file.json'))
            except Exception as e:
                self.fail('Unexpected exception {}'.format(e))

    @patch('process.scrapyd.spiders')
    def test_source_local(self, spiders):
        spiders.return_value = ['france']

        with self.settings(SCRAPYD={'url': 'http://example.com', 'project': 'kingfisher'}):
            try:
                call_command('load', '--source', 'france_local', '--note', 'x', '--force', path('file.json'))
            except Exception as e:
                self.fail('Unexpected exception {}'.format(e))

    def test_unique_deleted_at(self):
        source = collection(deleted_at='2001-01-01 00:00:00')
        source.save()

        with self.assertRaises(CommandError) as e:
            call_command('load', '--source', source.source_id, '--time', source.data_version, '--note', 'x',
                         path('file.json'))

        self.assertEqual(str(e.exception), 'A collection {} matching those arguments is being deleted'.format(
            source.pk))

    def test_unique_store_end_at(self):
        source = collection(store_end_at='2001-01-01 00:00:00')
        source.save()

        with self.assertRaises(CommandError) as e:
            call_command('load', '--source', source.source_id, '--time', source.data_version, '--note', 'x',
                         path('file.json'))

        self.assertEqual(str(e.exception), 'A closed collection {} matching those arguments already exists'.format(
            source.pk))

    def test_unique(self):
        source = collection()
        source.save()

        with self.assertRaises(CommandError) as e:
            call_command('load', '--source', source.source_id, '--time', source.data_version, '--note', 'x',
                         path('file.json'))

        self.assertEqual(str(e.exception), 'An open collection {0} matching those arguments already exists. Use '
                                           '--collection {0} to load data into it.'.format(source.pk))

# TODO: test files with: bad encoding, nested data
