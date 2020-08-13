import argparse

from django.core.management.base import BaseCommand, CommandError
from django.utils.translation import gettext as t
from django.utils.translation import gettext_lazy as _

from process.forms import CollectionForm, CollectionNoteForm
from process.models import Collection
from process.util import wrap as w


class Command(BaseCommand):
    help = w(t('Load data into a collection, asynchronously\n'
               'To load data into a new collection, set at least the --source and --note options. The --time option '
               'defaults to the earliest file creation time; if files were copied into place, set --time explicitly.\n'
               'The collection is automatically "closed" to new files. (Some processing steps like "compile-releases" '
               'require a collection to be closed.) To keep the collection "open" to new files, set --keep-open.\n'
               "To load data into an *open* collection, set --collection to the collection's ID, and set --keep-open "
               'until the last load. If you forget to remove --keep-open for the last load, use the endload command '
               'to close it.\n'
               'All files must have the same encoding (default UTF-8). If some files have different encodings, keep '
               'the collection open as above, and separately load the files with each encoding, using the --encoding '
               'option.\n'
               'The formats of files are automatically detected (release package, record package, release, record, '
               'compiled release), including JSON arrays and concatenated JSON of these. If OCDS data is embedded '
               'within files, use the --root-path option to indicate the path to the OCDS data to process within the '
               'files. For example, if release packages are in an array under a "results" key, use: --root-path '
               'results.item\n'
               'Additional processing is not automatically configured (checking, upgrading, merging, etc.). To add a '
               'pre-processing step, use the addstep command.'))

    def add_arguments(self, parser):
        parser.formatter_class = argparse.RawDescriptionHelpFormatter
        parser.add_argument('file', help=_('files or directories to load'), nargs='+')
        parser.add_argument('-s', '--source', help=_('the source from which the files were retrieved, if loading into '
                                                     'a new collection'))
        parser.add_argument('-t', '--time', help=_('the datetime at which the files were retrieved, if loading into a '
                                                   'new collection (defaults to the earliest file creation time)'))
        parser.add_argument('--sample', help=_('whether the files represent a sample from the source, if loading into '
                                               'a new collection'), action='store_true')
        parser.add_argument('--encoding', help=_('the encoding of all files (default UTF-8)'))
        parser.add_argument('--root-path', help=_('the path to the OCDS data to process within all files'))
        parser.add_argument('--keep-open', help=_('keep the collection "open" to new files'), action='store_true')
        parser.add_argument('--collection', help=_('the collection ID, if loading into an open collection'))
        parser.add_argument('-n', '--note', help=_('add a note to the collection (required for a new collection)'))

    def handle(self, *args, **options):
        if not options['collection'] and not options['source']:
            raise CommandError(_('Please indicate either a new collection (using --source and --note and, optionally, '
                                 '--time and --sample) or an open collection (using --collection)'))

        if options['collection'] and (options['source'] or options['time'] or options['sample']):
            raise CommandError(_('You cannot mix options for a new collection (--source, --time, --sample) and for an '
                                 'open collection (--collection)'))

        if options['source'] and not options['note']:
            raise CommandError(_('You must add a note (using --note) when loading into a new collection'))

        if options['collection']:
            collection_id = options['collection']
            try:
                collection = Collection.objects.get(pk=collection_id)
            except Collection.DoesNotExist:
                raise CommandError(_('Collection %(id)s does not exist') % {'id': collection_id})
            if collection.deleted_at:
                raise CommandError(_('Collection %(id)s is being deleted') % {'id': collection_id})
            if collection.store_end_at:
                raise CommandError(_('Collection %(id)s is closed to new files. If you need to re-open it, please '
                                     'comment at https://github.com/open-contracting/kingfisher-process/issues/276')
                                   % {'id': collection_id})
        else:
            data = {'source': options['source'], 'data_version': options['time'], 'sample': options['sample']}
            form = CollectionForm(data)

            if form.has_error('source_id'):
                # TODO: Suggest to the user to load data into the existing collection, if there is one that is open and
                # not deleted. Prompt for confirmation.

                # TODO: If the source_id is not recognized, offer the user to select a source_id from a list. (We can
                # perhaps use a "did you mean" library.)
                pass

            if form.is_valid():
                collection = form.save()
            else:
                raise CommandError(form.error_messages)

        if options['note']:
            form = CollectionNoteForm(dict(collection=collection, note=options['note']))
            if form.is_valid():
                form.save()
            else:
                raise CommandError(form.error_messages)

        # TODO: This command is incomplete. Partial work was committed to allow others to continue.
