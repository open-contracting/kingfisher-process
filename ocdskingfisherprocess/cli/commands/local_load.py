import ocdskingfisherprocess.database
import ocdskingfisherprocess.cli.commands.base
from ocdskingfisherprocess.store import Store
import os
import glob


class CheckCLICommand(ocdskingfisherprocess.cli.commands.base.CLICommand):
    command = 'local-load'

    def configure_subparser(self, subparser):
        self.configure_subparser_for_selecting_existing_collection(subparser)
        subparser.add_argument("directory", help="Directory to load from")
        subparser.add_argument("filetype", help="Types of file in the directory")
        subparser.add_argument("--fileextension", help="File Extension to load.", default="json")
        subparser.add_argument("--encoding", help="File encoding", default="utf-8")

    def run_command(self, args):

        file_type = args.filetype
        directory = args.directory
        if not directory[-1] == '/':
            directory += '/'
        file_extension = args.fileextension
        encoding = args.encoding

        self.run_command_for_selecting_existing_collection(args)
        if file_type not in Store.ALLOWED_DATA_TYPES:
            print("We can not find the file type that you requested!")
            quit(-1)

        if not os.path.isdir(directory):
            print("We can not find the directory that you requested!")
            quit(-1)

        store = Store(config=self.config, database=self.database)
        store.set_collection(self.collection)

        glob_path = os.path.join(directory, '*')
        for file_path in glob.glob(glob_path):

            throw_away, current_file_extension = os.path.splitext(file_path)
            if "."+file_extension.lower() == current_file_extension.lower():
                print("Processing {}".format(file_path))
                store.store_file_from_local(
                    file_path[len(directory):],
                    'file:/'+file_path,
                    file_type,
                    encoding,
                    file_path
                )
            else:
                print("Skipping {}".format(file_path))

        print("Done")
