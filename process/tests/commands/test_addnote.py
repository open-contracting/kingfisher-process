import datetime

from django.core.management import call_command
from django.core.management.base import CommandError
from django.test import TransactionTestCase

from process.models import CollectionNote
from process.tests.fixtures import collection


class AddnoteTests(TransactionTestCase):
    def test_exists(self):
        with self.assertRaises(CommandError) as e:
            call_command('addnote', '0', 'A note')

        self.assertEqual(str(e.exception), 'Collection 0 does not exist')

    def test_success(self):
        source = collection()
        source.save()

        call_command('addnote', source.pk, 'A note')

        source.refresh_from_db()

        self.assertEqual(source.collectionnote_set.count(), 1)

        collection_note = source.collectionnote_set.first()

        self.assertEqual(collection_note.collection_id, source.pk)
        self.assertEqual(collection_note.note, 'A note')
        self.assertAlmostEqual(collection_note.stored_at, datetime.datetime.utcnow(),
                               delta=datetime.timedelta(seconds=1))

    def test_empty(self):
        source = collection()
        source.save()

        with self.assertRaises(CommandError) as e:
            call_command('addnote', source.pk, '   ')

        self.assertEqual(str(e.exception), 'note "   " cannot be blank')

    def test_duplicate(self):
        source = collection()
        source.save()

        collection_note = CollectionNote(collection=source, note='A note')
        collection_note.save()

        with self.assertRaises(CommandError) as e:
            call_command('addnote', source.pk, 'A note')

        self.assertEqual(str(e.exception), 'Collection {} already has the note "A note"'.format(source.pk))
