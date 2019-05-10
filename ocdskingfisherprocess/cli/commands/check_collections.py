import logging

from ocdskingfisherprocess.cli.commands.base import TimeLimitedCLICommand
from ocdskingfisherprocess.cli.util import time_limit
from ocdskingfisherprocess.checks import Checks

logger = logging.getLogger('ocdskingfisherprocess')


class CheckCollectionsCLICommand(TimeLimitedCLICommand):
    command = 'check-collections'

    def run_command(self, args):
        with time_limit(args.runforseconds, self.command):
            for collection in self.database.get_all_collections():
                logger.info('Checking collection: {}'.format(collection.database_id))
                checks = Checks(self.database, collection)
                checks.process_all_files()
