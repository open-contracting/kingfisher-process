import glob
import os

import ocdskingfisherprocess.cli.commands.base
import ocdskingfisherprocess.database
from ocdskingfisherprocess.store import Store


class CheckCLICommand(ocdskingfisherprocess.cli.commands.base.CLICommand):
    command = 'local-load'

    def configure_subparser(self, subparser):
        self.configure_subparser_for_selecting_existing_collection(subparser)
        subparser.add_argument("directory", help="Directory to load from")
        subparser.add_argument("filetype", help="Types of file in the directory")
        subparser.add_argument("--encoding", help="File encoding", default="utf-8")
        subparser.add_argument("--keep-collection-store-open",
                               help="Keep collection store open (default is to end it straight after)",
                               default=False,
                               action='store_true')

    def run_command(self, args):

        file_type = args.filetype
        directory = os.path.abspath(args.directory)
        if not directory[-1] == '/':
            directory += '/'
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
            print("Processing {}".format(file_path))
            store.store_file_from_local(
                file_path[len(directory):],
                'file:/'+file_path,
                file_type,
                encoding,
                file_path
            )

        print("Done")

        if args.keep_collection_store_open:
            print("Not ending collection store as requested; you may want to use the end-collection-store command")
        else:
            self.database.mark_collection_store_done(self.collection.database_id)
            print("And collection store ended!")
