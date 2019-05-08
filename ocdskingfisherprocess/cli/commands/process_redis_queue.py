import logging

import redis

from ocdskingfisherprocess.cli.commands.base import TimeLimitedCLICommand
from ocdskingfisherprocess.cli.util import time_limit
from ocdskingfisherprocess.redis import ProcessQueueMessage

logger = logging.getLogger('ocdskingfisherprocess')


class ProcessRedisQueueCLICommand(TimeLimitedCLICommand):
    command = 'process-redis-queue'

    def run_command(self, args):
        if not self.config.is_redis_available():
            logger.error("No Redis is configured!")
            return

        redis_conn = redis.Redis(host=self.config.redis_host, port=self.config.redis_port, db=self.config.redis_database)
        process_que_message = ProcessQueueMessage(database=self.database)

        with time_limit(args.runforseconds):
            while True:
                data = redis_conn.blpop('kingfisher_work', timeout=10)
                if data:
                    message = data[1].decode('ascii')
                    logger.info('Got Message: {}'.format(message))
                    process_que_message.process(message)
                    logger.info('Processed!')
