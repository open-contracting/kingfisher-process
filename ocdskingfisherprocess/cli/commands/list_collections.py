import ocdskingfisherprocess.cli.commands.base


class ListCollections(ocdskingfisherprocess.cli.commands.base.CLICommand):
    command = 'list-collections'

    def configure_subparser(self, subparser):
        pass

    def run_command(self, args):

        collections = self.database.get_all_collections()

        print("{:5} {:40} {:20} {:5}".format(
            "DB-ID", "SOURCE-ID", "DATA-VERSION", "SAMPLE"
        ))

        for collection in collections:
            print("{:5} {:40} {:20} {:5}".format(
                    collection.database_id,
                    collection.source_id[:40],
                    collection.data_version,
                    ("Sample" if collection.sample else "Full")
                ))
