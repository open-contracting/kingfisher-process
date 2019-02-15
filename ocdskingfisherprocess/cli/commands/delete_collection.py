import ocdskingfisherprocess.cli.commands.base


class DeleteCollectionCLICommand(ocdskingfisherprocess.cli.commands.base.CLICommand):
    command = 'delete-collection'

    def configure_subparser(self, subparser):
        self.configure_subparser_for_selecting_existing_collection(subparser)

    def run_command(self, args):
        self.run_command_for_selecting_existing_collection(args)

        collection_id = self.collection.database_id

        if self.database.is_collection_source_for_a_transform(collection_id):
            print("Collection has a transform. It can't be deleted.")
            return

        self.database.mark_collection_deleted_at(collection_id)
