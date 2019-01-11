import ocdskingfisherprocess.database
import ocdskingfisherprocess.cli.commands.base
from ocdskingfisherprocess.transform.compile_releases import CompileReleasesTransform


class TransformCollectionCLICommand(ocdskingfisherprocess.cli.commands.base.CLICommand):
    command = 'transform-collection'

    def configure_subparser(self, subparser):
        self.configure_subparser_for_selecting_existing_collection(subparser)

    def run_command(self, args):

        self.run_command_for_selecting_existing_collection(args)

        if not self.collection.transform_type:
            print("That collection does not have any transforms!")
            quit(-1)

        transform = CompileReleasesTransform(self.config, self.database, self.collection)
        transform.process()
