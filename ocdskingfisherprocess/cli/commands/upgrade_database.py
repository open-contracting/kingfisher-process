import ocdskingfisherprocess.cli.commands.base


class UpgradeDataBaseCLICommand(ocdskingfisherprocess.cli.commands.base.CLICommand):
    command = 'upgrade-database'

    def configure_subparser(self, subparser):
        subparser.add_argument("--deletefirst", help="Delete Database First", action="store_true")

    def run_command(self, args):

        if args.deletefirst:
            if not args.quiet:
                print("Dropping Database")
            self.database.delete_tables()

        if not args.quiet:
            print("Upgrading/Creating Database")
        self.database.create_tables()
