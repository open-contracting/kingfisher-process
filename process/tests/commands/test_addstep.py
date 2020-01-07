import datetime

from django.core.management import call_command
from django.core.management.base import CommandError
from django.test import TransactionTestCase

from process.tests.fixtures import collection


class ProcessTests(TransactionTestCase):
    def test_exists(self):
        with self.assertRaises(CommandError) as e:
            call_command('addstep', '0', 'compile-releases')

        self.assertEqual(str(e.exception), 'Collection 0 does not exist')

    # The following tests mirror those in `test_models.py` for `Collection.clean_fields()`.

    def test_check(self):
        source = collection()
        source.save()
        call_command('addstep', source.id, 'check')

        source.refresh_from_db()

        self.assertTrue(source.steps['check'])

    def test_transform(self):
        source = collection()
        source.save()

        for transform_type in ('compile-releases', 'upgrade-1-0-to-1-1'):
            with self.subTest(transform_type=transform_type):
                call_command('addstep', source.id, transform_type)

                source.refresh_from_db()
                transforms = source.collection_set.filter(transform_type=transform_type)

                self.assertTrue(source.steps[transform_type])
                self.assertEqual(len(transforms), 1)
                self.assertEqual(transforms[0].source_id, 'example')
                self.assertEqual(transforms[0].data_version, datetime.datetime(2001, 1, 1, 0, 0))
                self.assertFalse(transforms[0].sample)
                self.assertEqual(transforms[0].parent_id, source.id)
                self.assertEqual(transforms[0].transform_type, transform_type)

    def test_deleted_at(self):
        source = collection(deleted_at='2001-01-01 00:00:00')
        source.save()

        with self.assertRaises(CommandError) as e:
            call_command('addstep', source.id, 'compile-releases')

        source.refresh_from_db()

        self.assertEqual(str(e.exception), 'Parent collection {} is being deleted'.format(source.id))
        self.assertNotIn('compile-releases', source.steps)

    def test_double_transform(self):
        source = collection()
        source.save()

        values = {
            'compile-releases': 'Parent collection {} is itself already a compilation of {}',
            'upgrade-1-0-to-1-1': 'Parent collection {} is itself already an upgrade of {}',
        }
        for transform_type, message in values.items():
            with self.subTest(transform_type=transform_type):
                original = collection(parent=source, transform_type=transform_type)
                original.save()

                with self.assertRaises(CommandError) as e:
                    call_command('addstep', original.id, transform_type)

                original.refresh_from_db()

                self.assertEqual(str(e.exception), message.format(original.id, original.parent_id))
                self.assertNotIn(transform_type, original.steps)

    def test_disallowed_transition(self):
        source = collection()
        source.save()

        compiled = collection(parent=source, transform_type='compile-releases')
        compiled.save()

        with self.assertRaises(CommandError) as e:
            call_command('addstep', compiled.id, 'upgrade-1-0-to-1-1')

        compiled.refresh_from_db()

        message = "Parent collection {} is compiled and can't be upgraded"
        self.assertEqual(str(e.exception), message.format(compiled.id))
        self.assertNotIn('upgrade-1-0-to-1-1', compiled.steps)

    def test_duplicate(self):
        source = collection()
        source.save()

        destination = collection(parent=source, transform_type='compile-releases')
        destination.save()

        with self.assertRaises(CommandError) as e:
            call_command('addstep', source.id, 'compile-releases')

        destination.refresh_from_db()

        message = 'Parent collection {} is already transformed into {}'
        self.assertEqual(str(e.exception), message.format(source.id, destination.id))
        self.assertNotIn('compile-releases', destination.steps)
