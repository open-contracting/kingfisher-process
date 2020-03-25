import ocdskingfisherprocess.database
import ocdskingfisherprocess.cli.commands.base
from ocdskingfisherprocess.transform import TRANSFORM_TYPE_COMPILE_RELEASES


class NewTransformCompileReleasesCLICommand(ocdskingfisherprocess.cli.commands.base.CLICommand):
    command = 'new-transform-compile-releases'

    def configure_subparser(self, subparser):
        self.configure_subparser_for_selecting_existing_collection(subparser)
        subparser.add_argument("--alwayscompile",
                               help="Always compile a release (only works if transform does not already exist)",
                               default=False, action='store_true')
        subparser.add_argument("--useexisting",
                               help="Use existing compiled releases, if they exist. (only works if transform does not already exist)",  # noqa
                               default=False,
                               action='store_true')

    def run_command(self, args):
        self.run_command_for_selecting_existing_collection(args)

        if args.useexisting and args.alwayscompile:
            print("You can't have both alwayscompile and useexisting")
            return

        if self.collection.deleted_at:
            print("That collection is deleted!")
            return

        id = self.database.get_collection_id(
            self.collection.source_id,
            self.collection.data_version,
            self.collection.sample,
            transform_from_collection_id=self.collection.database_id,
            transform_type=TRANSFORM_TYPE_COMPILE_RELEASES)
        if id:
            if not args.quiet:
                print("Already exists! The ID is {}".format(id))
            return

        options = {}
        if args.useexisting:
            options['transform-use-existing-compiled-releases'] = True
        elif args.alwayscompile:
            options['transform-use-existing-compiled-releases'] = False

        id = self.database.get_or_create_collection_id(self.collection.source_id,
                                                       self.collection.data_version,
                                                       self.collection.sample,
                                                       transform_from_collection_id=self.collection.database_id,
                                                       transform_type=TRANSFORM_TYPE_COMPILE_RELEASES,
                                                       create_options=options)

        if not args.quiet:
            print("Created! The ID is {}".format(id))
            print("Now run transform-collection with that ID.")
