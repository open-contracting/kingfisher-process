import ocdskingfisherprocess.database
import ocdskingfisherprocess.cli.commands.base


class NewCollectionNoteCLICommand(ocdskingfisherprocess.cli.commands.base.CLICommand):
    command = 'new-collection-note'

    def configure_subparser(self, subparser):
        self.configure_subparser_for_selecting_existing_collection(subparser)
        subparser.add_argument("note", help="Note")

    def run_command(self, args):
        self.run_command_for_selecting_existing_collection(args)
        note = args.note.strip()
        if note:
            self.database.add_collection_note(self.collection.database_id, note)
            if not args.quiet:
                print("Done")
