import ocdskingfisherprocess.database
import ocdskingfisherprocess.cli.commands.base
from ocdskingfisherprocess.checks import Checks


class CheckCLICommand(ocdskingfisherprocess.cli.commands.base.CLICommand):
    command = 'check-collection'

    def configure_subparser(self, subparser):
        self.configure_subparser_for_selecting_existing_collection(subparser)

    def run_command(self, args):

        self.run_command_for_selecting_existing_collection(args)

        checks = Checks(self.database, self.collection)
        checks.process_all_files()
