from django.core.management.base import CommandError
from django.utils.translation import gettext
from django.utils.translation import gettext_lazy as _

from process.cli import CollectionCommand
from process.forms import CollectionNoteForm


class Command(CollectionCommand):
    help = gettext('Adds a note to a collection')

    def add_collection_arguments(self, parser):
        parser.add_argument('note', help=_('the note'))

    def handle_collection(self, collection, *args, **options):
        form = CollectionNoteForm(dict(collection=collection, note=options['note']))
        if form.is_valid():
            form.save()
        else:
            raise CommandError(form.error_messages)
