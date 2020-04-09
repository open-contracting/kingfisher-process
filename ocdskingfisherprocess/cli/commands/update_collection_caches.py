import logging

import ocdskingfisherprocess.cli.commands.base
import ocdskingfisherprocess.database


class UpdateCollectionCachesCLICommand(ocdskingfisherprocess.cli.commands.base.CLICommand):
    command = 'update-collection-caches'

    def run_command(self, args):
        logger = logging.getLogger('ocdskingfisher.cli.update-collection-caches')
        logger.info("Starting command")

        for collection in self.database.get_all_collections():
            if collection.store_end_at:
                if not args.quiet:
                    print("Collection " + str(collection.database_id))
                logger.info("Starting to update caches for collection " + str(collection.database_id))
                self.database.update_collection_cached_columns(collection.database_id)
