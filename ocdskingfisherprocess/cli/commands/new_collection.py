import ocdskingfisherprocess.database
import ocdskingfisherprocess.cli.commands.base
from ocdskingfisherprocess.util import parse_string_to_date_time


class CheckCLICommand(ocdskingfisherprocess.cli.commands.base.CLICommand):
    command = 'new-collection'

    def configure_subparser(self, subparser):
        subparser.add_argument("sourceid", help="Source ID, a string")
        subparser.add_argument("date", help="Date collection started, in format YYYY-MM-DD HH:MM:SS")
        subparser.add_argument("--sample", help="Sample", default=False, action='store_true')

    def run_command(self, args):
        source_id = args.sourceid
        data_version = parse_string_to_date_time(args.date)
        sample = args.sample

        id = self.database.get_collection_id(source_id, data_version, sample)
        if id:
            print("Already exists! The ID is {}".format(id))
            return

        id = self.database.get_or_create_collection_id(source_id, data_version, sample)
        print("Created! The ID is {}".format(id))
