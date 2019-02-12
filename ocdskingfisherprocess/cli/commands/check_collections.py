import ocdskingfisherprocess.database
import ocdskingfisherprocess.cli.commands.base
from ocdskingfisherprocess.checks import Checks
import datetime
from threading import Timer
import os


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

            # This is a safeguard - the process should stop itself but this will kill it if it does not.
            def exitfunc():
                os._exit(0)

            Timer(run_for_seconds + 60, exitfunc).start()

        for collection in self.database.get_all_collections():
            if not args.quiet:
                print("Collection " + str(collection.database_id))
            checks = Checks(self.database, collection, run_until_timestamp=run_until_timestamp)
            checks.process_all_files()
            # Early return?
            if run_until_timestamp and run_until_timestamp < datetime.datetime.utcnow().timestamp():
                break

        # If the code above took less than 60 seconds the process will stay open, waiting for the Timer to execute.
        # So just kill it to make sure.
        os._exit(0)
