import datetime
from threading import Timer
import os
import logging
import json

import redis

import ocdskingfisherprocess.cli.commands.base


class ProcessRedisQueueCollectionStoreFinishedCLICommand(ocdskingfisherprocess.cli.commands.base.CLICommand):
    command = 'process-redis-queue-collection-store-finished'

    def configure_subparser(self, subparser):
        subparser.add_argument("--runforseconds",
                               help="Run for this many seconds only.")

    def run_command(self, args):
        if not self.config.is_redis_available():
            print("No Redis is configured!")
            return

        run_until_timestamp = None
        run_for_seconds = int(args.runforseconds) if args.runforseconds else 0
        if run_for_seconds > 0:
            run_until_timestamp = datetime.datetime.utcnow().timestamp() + run_for_seconds

            # This is a safeguard - the process should stop itself but this will kill it if it does not.
            def exitfunc():
                os._exit(0)

            Timer(run_for_seconds + 60, exitfunc).start()

        redis_conn = redis.Redis(
            host=self.config.redis_host,
            port=self.config.redis_port,
            db=self.config.redis_database
        )
        logger = logging.getLogger('ocdskingfisher.redis-queue-collection-store-finished')

        run = True
        while run:
            data = redis_conn.blpop("kingfisher_work_collection_store_finished", timeout=10)
            if data:
                message = json.loads(data[1].decode('ascii'))
                if not args.quiet:
                    print("Got Collection: " + str(message.get('collection_id')))
                logger.info("Got Collection: " + str(message.get('collection_id')))
                self.database.update_collection_cached_columns(message.get('collection_id'))
                if not args.quiet:
                    print("Processed!")
            # Early return?
            if run_until_timestamp and run_until_timestamp < datetime.datetime.utcnow().timestamp():
                run = False

        # If the code above took less than 60 seconds the process will stay open, waiting for the Timer to execute.
        # So just kill it to make sure.
        os._exit(0)
