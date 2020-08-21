import argparse
import os
import time

from django.core.management.base import BaseCommand, CommandError
from django.db.utils import IntegrityError
from django.utils.translation import gettext as t
from django.utils.translation import gettext_lazy as _

from process.forms import CollectionForm, CollectionNoteForm
from process.models import Collection
from process.scrapyd import configured
from process.util import walk
from process.util import wrap as w


def file_or_directory(string):
    # Help the user to avoid a typo.
    if not os.path.exists(string):
        raise argparse.ArgumentTypeError(t('No such file or directory %(path)r') % {'path': string})
    return string


class Command(BaseCommand):
    help = w(t('Load data into a collection, asynchronously\n'
               'To load data into a new collection, set at least --source and --note. --time defaults to the earliest '
               'file modification time; if files were copied into place, set --time explicitly.\n'
               'The collection is automatically "closed" to new files. (Some processing steps like "compile-releases" '
               'require a collection to be closed.) To keep the collection "open" to new files, set --keep-open.\n'
               "To load data into an *open* collection, set --collection to the collection's ID, and set --keep-open "
               'until the last load. If you forget to remove --keep-open for the last load, use the endload command '
               'to close it.\n'
               'All files must have the same encoding (default UTF-8). If some files have different encodings, keep '
               'the collection open as above, and separately load the files with each encoding, using --encoding.\n'
               'The formats of files are automatically detected (release package, record package, release, record, '
               'compiled release), including JSON arrays and concatenated JSON of these. If OCDS data is embedded '
               'within files, use --root-path to indicate the path to the OCDS data to process within the files. For '
               'example, if release packages are in an array under a "results" key, use: --root-path results.item\n'
               'Additional processing is not automatically configured (checking, upgrading, merging, etc.). To add a '
               'pre-processing step, use the addstep command.'))

    def add_arguments(self, parser):
        parser.formatter_class = argparse.RawDescriptionHelpFormatter
        parser.add_argument('PATH', help=_('a file or directory to load'), nargs='+', type=file_or_directory)
        parser.add_argument('-s', '--source', help=_('the source from which the files were retrieved, if loading into '
                                                     'a new collection (please append "_local" if the data was not '
                                                     'collected by Kingfisher Collect)'))
        parser.add_argument('-t', '--time', help=_('the time at which the files were retrieved, if loading into a new '
                                                   'collection, in "YYYY-MM-DD HH:MM:SS" format (defaults to the '
                                                   'earliest file modification time)'))
        parser.add_argument('--sample', help=_('whether the files represent a sample from the source, if loading into '
                                               'a new collection'), action='store_true')
        parser.add_argument('--encoding', help=_('the encoding of all files (defaults to UTF-8)'))
        parser.add_argument('--root-path', help=_('the path to the OCDS data to process within all files'))
        parser.add_argument('--keep-open', help=_('keep the collection "open" to new files'), action='store_true')
        parser.add_argument('--collection', help=_('the collection ID, if loading into an open collection'), type=int)
        parser.add_argument('-n', '--note', help=_('add a note to the collection (required for a new collection)'))
        parser.add_argument('-f', '--force', help=_('use the provided --source value, regardless of whether it is '
                                                    'recognized'), action='store_true')

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
            if not configured():
                self.stderr.write(self.style.WARNING("The --source argument can't be validated, because a Scrapyd URL "
                                                     "is not configured in settings.py."))

            mtimes = [os.path.getmtime(path) for path in walk(options['PATH'])]
            if not mtimes:
                raise CommandError(_('No files found'))

            data_version = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(min(mtimes)))
            if options['time']:
                if options['time'] > data_version:
                    raise CommandError(_('%(time)r is greater than the earliest file modification time: %(mtime)r') % {
                        'time': options['time'], 'mtime': data_version})
                data_version = options['time']

            data = {'source_id': options['source'], 'data_version': data_version, 'sample': options['sample'],
                    'force': options['force']}
            form = CollectionForm(data)

            if form.is_valid():
                try:
                    collection = form.save()
                except IntegrityError:
                    del data['force']  # force is a field on the form, not the model
                    collection = Collection.objects.get(**data, transform_type='')
                    if collection.deleted_at:
                        message = _('A collection %(id)s matching those arguments is being deleted')
                    elif collection.store_end_at:
                        message = _('A closed collection %(id)s matching those arguments already exists')
                    else:
                        message = _('An open collection %(id)s matching those arguments already exists. Use '
                                    '--collection %(id)s to load data into it.')
                    raise CommandError(message % {'id': collection.pk})
            else:
                if form.has_error('source_id', 'invalid_choice'):
                    self.stderr.write(self.style.ERROR(_('Use --force to ignore the following error:')))
                raise CommandError(form.error_messages)

        if options['note']:
            form = CollectionNoteForm(dict(collection=collection, note=options['note']))
            if form.is_valid():
                form.save()
            else:
                raise CommandError(form.error_messages)

        # TODO: This command is incomplete. Partial work was committed to allow others to continue.

        # TODO: If format can't be determined, skip with a warning, and leave the collection open?
        # What to do about binary and non-JSON files? (If we only load .json files, we'll miss .jsonl, etc. files)
        # Should ocdskit's detect-format be improved to ignore files whose first non-whitespace character isn't [ or {?
        # Should ocdskit's detect-format warn about a BOM?
