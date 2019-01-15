from ocdskingfisherprocess.transform.base import BaseTransform
import sqlalchemy as sa
import ocdsmerge


class CompileReleasesTransform(BaseTransform):
    type = 'compile-releases'

    def process(self):
        for ocid in self.get_ocids():
            if not self.has_ocid_been_transformed(ocid):
                self.process_ocid(ocid)

    def get_ocids(self):
        ocids = []

        with self.database.get_engine().begin() as engine:
            query = engine.execute(sa.text(
                " SELECT release.ocid FROM release " +
                " JOIN collection_file_item ON  collection_file_item.id = release.collection_file_item_id " +
                " JOIN collection_file ON collection_file.id = collection_file_item.collection_file_id  " +
                " WHERE collection_file.collection_id = :collection_id " +
                " GROUP BY release.ocid "
            ), collection_id=self.source_collection.database_id)

            for row in query:
                ocids.append(row['ocid'])

        return ocids

    def has_ocid_been_transformed(self, ocid):

        with self.database.get_engine().begin() as engine:
            query = engine.execute(sa.text(
                " SELECT compiled_release.ocid FROM compiled_release " +
                " JOIN collection_file_item ON  collection_file_item.id = compiled_release.collection_file_item_id " +
                " JOIN collection_file ON collection_file.id = collection_file_item.collection_file_id  " +
                " WHERE collection_file.collection_id = :collection_id AND compiled_release.ocid = :ocid "
            ), collection_id=self.destination_collection.database_id, ocid=ocid)

            return query.rowcount == 1

    def process_ocid(self, ocid):

        releases = []

        with self.database.get_engine().begin() as engine:
            query = engine.execute(sa.text(
                " SELECT release.* FROM release " +
                " JOIN collection_file_item ON  collection_file_item.id = release.collection_file_item_id " +
                " JOIN collection_file ON collection_file.id = collection_file_item.collection_file_id  " +
                " WHERE collection_file.collection_id = :collection_id AND release.ocid = :ocid "
            ), collection_id=self.source_collection.database_id, ocid=ocid)

            for row in query:
                releases.append(self.database.get_data(row['data_id']))

        out = ocdsmerge.merge(releases)

        self.store.store_file_item(ocid+'.json', None, 'compiled_release', out, 1)
