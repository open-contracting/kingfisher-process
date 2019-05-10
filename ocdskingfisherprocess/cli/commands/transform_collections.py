import logging

from ocdskingfisherprocess.cli.commands.base import TimeLimitedCLICommand
from ocdskingfisherprocess.cli.util import time_limit
from ocdskingfisherprocess.transform.util import get_transform_instance

logger = logging.getLogger('ocdskingfisherprocess')


class TransformCollectionsCLICommand(TimeLimitedCLICommand):
    command = 'transform-collections'

    def run_command(self, args):
        with time_limit(args.runforseconds, self.command):
            for collection in self.database.get_all_collections():
                if collection.transform_type:
                    logger.info('Transforming collection: {}'.format(collection.database_id))
                    transform = get_transform_instance(collection.transform_type, self.config, self.database,
                                                       collection)
                    transform.process()
