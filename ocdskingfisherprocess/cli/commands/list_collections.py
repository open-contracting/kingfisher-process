import ocdskingfisherprocess.cli.commands.base


class ListCollections(ocdskingfisherprocess.cli.commands.base.CLICommand):
    command = 'list-collections'

    def configure_subparser(self, subparser):
        pass

    def run_command(self, args):

        collections = self.database.get_all_collections()

        print("{:5} {:40} {:20} {:5} {:20} {:5} {:8}".format(
            "DB-ID", "SOURCE-ID", "DATA-VERSION", "SAMPLE", "TRANSFORM", "FROM", "DELETED"
        ))

        for collection in collections:
            print("{:5} {:40} {:20} {:5} {:20} {:5} {:7}".format(
                    collection.database_id,
                    collection.source_id[:40],
                    collection.data_version.strftime("%Y-%m-%d %H:%M:%S"),
                    ("Sample" if collection.sample else "Full"),
                    collection.transform_type if collection.transform_type else "-",
                    collection.transform_from_collection_id if collection.transform_from_collection_id else "",
                    ("DELETED" if collection.deleted_at else "-"),
                ))
