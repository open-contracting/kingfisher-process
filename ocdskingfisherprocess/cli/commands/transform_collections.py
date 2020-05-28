import concurrent.futures
import datetime
import logging
import os
import traceback
from threading import Timer

import sentry_sdk

import ocdskingfisherprocess.cli.commands.base
import ocdskingfisherprocess.database
from ocdskingfisherprocess.transform.util import get_transform_instance


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
            logger = logging.getLogger('ocdskingfisher.cli.transform-collections')
            logger.info("Starting to transform collection " + str(collection.database_id))
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

        # Make sure Engine is created once upfront, before threading starts, as there
        # is a race condition where multiple Engines could be made.
        # Engine itself and the its default QueuePool should be threadsafe.
        self.database.get_engine()

        with concurrent.futures.ThreadPoolExecutor(max_workers=args.threads) as executor:
            futures = [
                executor.submit(
                    self.run_collection, collection, run_until_timestamp, args
                )
                for collection in self.database.get_all_collections()
                # Lets keep number of possible threads low!
                # Only if is a transform and works needs doing
                # [ There are more things than just "not collection.store_end_at" to check
                #    to work out if "works needs doing" but
                #    A) We don't want to duplicate lots of them here
                #    B) They vary by type and are complex
                #    C) "not collection.store_end_at" should catch a lot of collections, that will do us for now ]
                if collection.transform_type and not collection.store_end_at
            ]

            for future in concurrent.futures.as_completed(futures):
                continue

        # If the code above took less than 60 seconds the process will stay open, waiting for the Timer to execute.
        # So just kill it to make sure.
        logger.info("Finishing command")
        os._exit(0)
