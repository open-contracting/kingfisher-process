import ocdskingfisherprocess.cli.commands.base
from ocdskingfisherprocess.util import parse_string_to_date_time


class DeleteCollectionCLICommand(ocdskingfisherprocess.cli.commands.base.CLICommand):
    command = 'delete-collection'

    def configure_subparser(self, subparser):
        subparser.add_argument("collection", help="Collection ID (Use list-collections command to find the ID)")
        subparser.add_argument("date", help="Date in format YYYY-MM-DD HH:MM:SS")

    def run_command(self, args):
        self.collection = self.database.get_collection(args.collection)

        if self.collection.transform_from_collection_id:
            print("Collection has a transform. It can't be deleted.")
            return

        self.collection.deleted_at = parse_string_to_date_time(args.date)
