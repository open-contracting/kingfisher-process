import ocdskingfisherprocess.database
import ocdskingfisherprocess.cli.commands.base
from ocdskingfisherprocess.transform import TRANSFORM_TYPE_UPGRADE_1_0_TO_1_1


class NewTransformUpgrade10To11CLICommand(ocdskingfisherprocess.cli.commands.base.CLICommand):
    command = 'new-transform-upgrade-1-0-to-1-1'

    def configure_subparser(self, subparser):
        self.configure_subparser_for_selecting_existing_collection(subparser)

    def run_command(self, args):
        self.run_command_for_selecting_existing_collection(args)

        if self.collection.deleted_at:
            print("That collection is deleted!")
            return

        id = self.database.get_collection_id(
            self.collection.source_id,
            self.collection.data_version,
            self.collection.sample,
            transform_from_collection_id=self.collection.database_id,
            transform_type=TRANSFORM_TYPE_UPGRADE_1_0_TO_1_1)
        if id:
            if not args.quiet:
                print("Already exists! The ID is {}".format(id))
            return

        id = self.database.get_or_create_collection_id(self.collection.source_id,
                                                       self.collection.data_version,
                                                       self.collection.sample,
                                                       transform_from_collection_id=self.collection.database_id,
                                                       transform_type=TRANSFORM_TYPE_UPGRADE_1_0_TO_1_1)

        if not args.quiet:
            print("Created! The ID is {}".format(id))
            print("Now run transform-collection with that ID.")
