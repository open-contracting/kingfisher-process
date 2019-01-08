from ocdskingfisherprocess.database import DataBase


class CLICommand:
    command = ''

    def __init__(self, config=None):
        self.collection_instance = None
        self.config = config
        self.database = DataBase(config=config)

    def configure_subparser(self, subparser):
        pass

    def run_command(self, args):
        pass

    def configure_subparser_for_selecting_existing_collection(self, subparser):
        subparser.add_argument("collection", help="Collection ID (Use list-collections command to find the ID)")

    def run_command_for_selecting_existing_collection(self, args):

        self.collection = self.database.get_collection(args.collection)
        if not self.collection:
            print("We can not find the collection that you requested!")
            quit(-1)
