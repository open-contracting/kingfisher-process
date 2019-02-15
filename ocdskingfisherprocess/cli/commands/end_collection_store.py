import ocdskingfisherprocess.database
import ocdskingfisherprocess.cli.commands.base


class EndCollectionStoreCLICommand(ocdskingfisherprocess.cli.commands.base.CLICommand):
    command = 'end-collection-store'

    def configure_subparser(self, subparser):
        self.configure_subparser_for_selecting_existing_collection(subparser)

    def run_command(self, args):

        self.run_command_for_selecting_existing_collection(args)

        if self.collection.store_end_at:
            if not args.quiet:
                print("Already Ended!")
            return

        self.database.mark_collection_store_done(self.collection.database_id)

        if not args.quiet:
            print("Store Ended!")
