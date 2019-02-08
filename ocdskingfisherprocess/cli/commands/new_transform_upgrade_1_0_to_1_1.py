import ocdskingfisherprocess.database
import ocdskingfisherprocess.cli.commands.base
from ocdskingfisherprocess.transform.upgrade_1_0_to_1_1 import Upgrade10To11Transform


class NewTransformUpgrade10To11CLICommand(ocdskingfisherprocess.cli.commands.base.CLICommand):
    command = 'new-transform-upgrade-1-0-to-1-1'

    def configure_subparser(self, subparser):
        self.configure_subparser_for_selecting_existing_collection(subparser)

    def run_command(self, args):
        self.run_command_for_selecting_existing_collection(args)

        id = self.database.get_collection_id(
            self.collection.source_id,
            self.collection.data_version,
            self.collection.sample,
            transform_from_collection_id=self.collection.database_id,
            transform_type=Upgrade10To11Transform.type)
        if id:
            if not args.quiet:
                print("Already exists! The ID is {}".format(id))
            return

        id = self.database.get_or_create_collection_id(self.collection.source_id,
                                                       self.collection.data_version,
                                                       self.collection.sample,
                                                       transform_from_collection_id=self.collection.database_id,
                                                       transform_type=Upgrade10To11Transform.type)

        if not args.quiet:
            print("Created! The ID is {}".format(id))
            print("Now run transform-collection with that ID.")
