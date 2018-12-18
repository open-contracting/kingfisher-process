import ocdskingfisherprocess.database
import ocdskingfisherprocess.cli.commands.base
from ocdskingfisherprocess.checks import Checks


class CheckCLICommand(ocdskingfisherprocess.cli.commands.base.CLICommand):
    command = 'check-collection'

    def configure_subparser(self, subparser):
        self.configure_subparser_for_selecting_existing_collection(subparser)
        subparser.add_argument("--schemaversion", help="Set Schema Version - defaults to 1.1")

    def run_command(self, args):

        self.run_command_for_selecting_existing_collection(args)

        override_schema_version = args.schemaversion

        schema_versions = ["1.0", "1.1"]
        if override_schema_version and override_schema_version not in schema_versions:
            print("We do not recognise that schema version! Options are: %s" % ", ".join(schema_versions))
            quit(-1)

        checks = Checks(self.database, self.collection, override_schema_version=override_schema_version)
        checks.process_all_files()
