from django.core.management import call_command
from django.core.management.base import CommandError
from django.test import TransactionTestCase

from default.tests.fixtures import collection


class ProcessTests(TransactionTestCase):
    def test_exists(self):
        with self.assertRaises(CommandError) as e:
            call_command('addtransform', '0', 'compile-releases')

        self.assertEqual(str(e.exception), 'Collection 0 does not exist')

    # The following tests mirror those in `test_models.py` for `Collection.clean_fields()`.

    def test_deleted_at(self):
        source = collection(deleted_at='2001-01-01 00:00:00')
        source.save()

        with self.assertRaises(CommandError) as e:
            call_command('addtransform', source.id, 'compile-releases')

        self.assertEqual(str(e.exception), 'Collection {} is being deleted'.format(source.id))

    def test_double_transform(self):
        source = collection()
        source.save()

        values = {
            'compile-releases': 'Collection {} is itself already a compilation of {}',
            'upgrade-1-0-to-1-1': 'Collection {} is itself already an upgrade of {}',
        }
        for transform_type, message in values.items():
            with self.subTest(transform_type=transform_type):
                upgrade = collection(parent=source, transform_type=transform_type)
                upgrade.save()

                with self.assertRaises(CommandError) as e:
                    call_command('addtransform', upgrade.id, transform_type)

                self.assertEqual(str(e.exception), message.format(upgrade.id, upgrade.parent_id))

    def test_disallowed_transition(self):
        source = collection()
        source.save()

        compiled = collection(parent=source, transform_type='compile-releases')
        compiled.save()

        with self.assertRaises(CommandError) as e:
            call_command('addtransform', compiled.id, 'upgrade-1-0-to-1-1')

        self.assertEqual(str(e.exception), "Collection {} is compiled and can't be upgraded".format(compiled.id))

    def test_duplicate(self):
        source = collection()
        source.save()

        obj = collection(parent=source, transform_type='compile-releases')
        obj.save()

        with self.assertRaises(CommandError) as e:
            call_command('addtransform', source.id, 'compile-releases')

        self.assertEqual(str(e.exception), 'Collection {} is already transformed into {}'.format(source.id, obj.id))
