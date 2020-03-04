import ocdskingfisherprocess.database
import ocdskingfisherprocess.cli.commands.base
import concurrent.futures
from ocdskingfisherprocess.transform.util import get_transform_instance
import datetime
import logging
import os
import traceback
from threading import Timer

import sentry_sdk

import ocdskingfisherprocess.cli.commands.base
import ocdskingfisherprocess.database
from ocdskingfisherprocess.transform.util import get_transform_instance

logger = logging.getLogger("ocdskingfisher.cli.transform-collections")


class TransformCollectionsCLICommand(ocdskingfisherprocess.cli.commands.base.CLICommand):
    command = "transform-collections"

    def configure_subparser(self, subparser):
        subparser.add_argument("--runforseconds",
                               help="Run for this many seconds only.")

        subparser.add_argument("--threads",
                               help="Amount of threads to use", type=int, default=1)

    def run_collection(self, collection, run_until_timestamp, args):
        # Early return?
        if run_until_timestamp and run_until_timestamp < datetime.datetime.utcnow().timestamp():
            return

        if collection.transform_type:
            if not args.quiet:
                print("Collection " + str(collection.database_id))
            if not args.quiet:
                print("Collection " + str(collection.database_id))
            transform = get_transform_instance(
                collection.transform_type,
                self.config,
                self.database,
                collection,
                run_until_timestamp=run_until_timestamp,
            )
            try:
                transform.process()
            except Exception as e:
                traceback.print_tb(e.__traceback__)
                with sentry_sdk.push_scope() as scope:
                    scope.set_tag("transform_collection", collection.database_id)
                    sentry_sdk.capture_exception(e)


    def run_command(self, args):
        logger = logging.getLogger('ocdskingfisher.cli.transform-collections')
        logger.info("Starting command")
        run_until_timestamp = None
        run_for_seconds = int(args.runforseconds) if args.runforseconds else 0
        if run_for_seconds > 0:
            run_until_timestamp = datetime.datetime.utcnow().timestamp() + run_for_seconds

            # This is a safeguard - the process should stop itself but this will kill it if it does not.
            def exitfunc():
                os._exit(0)

            Timer(run_for_seconds + 60, exitfunc).start()

        with concurrent.futures.ThreadPoolExecutor(max_workers=args.threads) as executor:
            futures = [
                executor.submit(
                    self.run_collection, collection, run_until_timestamp, args
                )
                for collection in self.database.get_all_collections()
            ]

            for future in concurrent.futures.as_completed(futures):
                continue

        # If the code above took less than 60 seconds the process will stay open, waiting for the Timer to execute.
        # So just kill it to make sure.
        logger.info("Finishing command")
        os._exit(0)
