from datetime import datetime

import ocdskingfisherprocess.cli.commands.base
from ocdskingfisherprocess.util import parse_string_to_date_time


class DeleteCollectionCLICommand(ocdskingfisherprocess.cli.commands.base.CLICommand):
    command = 'delete-collection'

    def configure_subparser(self, subparser):
        self.configure_subparser_for_selecting_existing_collection(subparser)

    def run_command(self, args):
        self.run_command_for_selecting_existing_collection(args)

        if self.database.check_collection_transform():
            print("Collection has a transform. It can't be deleted.")
            return

        self.collection.deleted_at = parse_string_to_date_time(datetime.utcnow())
