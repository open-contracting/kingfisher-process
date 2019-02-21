import ocdskingfisherprocess.cli.commands.base

import redis


class UpgradeDataBaseCLICommand(ocdskingfisherprocess.cli.commands.base.CLICommand):
    command = 'upgrade-database'

    def configure_subparser(self, subparser):
        subparser.add_argument("--deletefirst", help="Delete Database First", action="store_true")

    def run_command(self, args):

        if args.deletefirst:
            if not args.quiet:
                print("Dropping Database")
            self.database.delete_tables()
            if self.config.is_redis_available():
                if not args.quiet:
                    print("Dropping Redis")
                redis_conn = redis.Redis(host=self.config.redis_host, port=self.config.redis_port, db=self.config.redis_database)
                redis_conn.delete('kingfisher_work')

        if not args.quiet:
            print("Upgrading/Creating Database")
        self.database.create_tables()
