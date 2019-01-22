import ocdskingfisherprocess.database
import ocdskingfisherprocess.cli.commands.base
from ocdskingfisherprocess.checks import Checks
import datetime


class CheckCollectionsCLICommand(ocdskingfisherprocess.cli.commands.base.CLICommand):
    command = 'check-collections'

    def configure_subparser(self, subparser):
        subparser.add_argument("--runforseconds",
                               help="Run for this many seconds only.")

    def run_command(self, args):
        run_until_timestamp = None
        run_for_seconds = int(args.runforseconds) if args.runforseconds else 0
        if run_for_seconds > 0:
            run_until_timestamp = datetime.datetime.utcnow().timestamp() + run_for_seconds

        for collection in self.database.get_all_collections():
            checks = Checks(self.database, collection, run_until_timestamp=run_until_timestamp)
            checks.process_all_files()
