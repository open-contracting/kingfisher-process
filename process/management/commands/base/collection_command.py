from django.core.management.base import CommandError
from django.utils.translation import gettext_lazy as _

from process.management.commands.base.worker import BaseWorker
from process.models import Collection


class CollectionCommand(BaseWorker):

    def __init__(self, worker_name):
        super().__init__(worker_name)

    def add_arguments(self, parser):
        """
        Adds default arguments to the command.
        """
        parser.add_argument('collection_id', help=_('the ID of the collection'))
        self.add_collection_arguments(parser)

    def add_collection_arguments(self, parser):
        """
        Adds arguments specific to this command.
        """
        pass

    def handle(self, *args, **options):
        """
        Gets the collection.
        """
        collection_id = options['collection_id']

        try:
            collection = Collection.objects.get(pk=collection_id)
        except Collection.DoesNotExist:
            raise CommandError(_('Collection %(id)s does not exist') % {'id': collection_id})

        self.handle_collection(collection, *args, **options)

    def handle_collection(self, collection, *args, **options):
        """
        Runs the command.
        """
        raise NotImplementedError('collection commands must implement handle_collection()')
