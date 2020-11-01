import redis

import ocdskingfisherprocess.cli.commands.base


class UpgradeDataBaseCLICommand(ocdskingfisherprocess.cli.commands.base.CLICommand):
    command = 'upgrade-database'

    def configure_subparser(self, subparser):
        subparser.add_argument("--deletefirst", help="Delete Database First", action="store_true")

    def run_command(self, args):

        if args.deletefirst:
            print("Dropping Database")
            self.database.delete_tables()
            if self.config.is_redis_available():
                print("Dropping Redis")
                redis_conn = redis.Redis(
                    host=self.config.redis_host,
                    port=self.config.redis_port,
                    db=self.config.redis_database
                )
                redis_conn.delete('kingfisher_work')
                redis_conn.delete('kingfisher_work_collection_store_finished')

        print("Upgrading/Creating Database")
        self.database.create_tables()
