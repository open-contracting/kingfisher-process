from django.core.exceptions import ValidationError
from django.core.management.base import BaseCommand, CommandError
from django.utils.translation import gettext_lazy as _

from process.models import Collection


class Command(BaseCommand):
    help = _('Process a collection')

    def add_arguments(self, parser):
        parser.add_argument('collection_id', help=_('the collection ID'))
        parser.add_argument('transform_type', choices=Collection.Transforms.values, help=_('the transform to run'))

    def handle(self, *args, **options):
        collection_id = options['collection_id']
        transform_type = options['transform_type']

        try:
            source = Collection.objects.get(pk=collection_id)
        except Collection.DoesNotExist:
            raise CommandError(_('Collection %(source_id)s does not exist') % {'source_id': collection_id})

        try:
            source.add_transform(transform_type)
        except ValidationError as e:
            raise CommandError('\n'.join(e.messages))
