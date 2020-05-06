import logging

import ocdskingfisherprocess.cli.commands.base


class DeleteCollectionsCLICommand(ocdskingfisherprocess.cli.commands.base.CLICommand):
    command = 'delete-collections'

    def run_command(self, args):
        logger = logging.getLogger('ocdskingfisher.cli.delete-collections')
        logger.info("Starting command")
        for collection in self.database.get_all_collections():
            if collection.deleted_at is not None:
                if not args.quiet:
                    print("Collection " + str(collection.database_id))
                logger.info("Starting to delete collection " + str(collection.database_id))
                self.database.delete_collection(collection.database_id)
        print("Orphan Data")
        logger.info("Starting to delete orphan data")
        self.database.delete_orphan_data()
        logger.info("Finishing command")
