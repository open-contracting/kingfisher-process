from ocdskingfisherprocess.transform.base import BaseTransform
import sqlalchemy as sa
import ocdsmerge
import datetime


class CompileReleasesTransform(BaseTransform):

    def process(self):
        # Is deleted?
        if self.destination_collection.deleted_at:
            return

        # This transform can only run when the source collection is fully stored!
        if not self.source_collection.store_end_at:
            return

        # Have we already marked this transform as finished?
        if self.destination_collection.store_end_at:
            return

        # Do the work ...
        for ocid in self.get_ocids():
            if not self.has_ocid_been_transformed(ocid):
                self.process_ocid(ocid)
            # Early return?
            if self.run_until_timestamp and self.run_until_timestamp < datetime.datetime.utcnow().timestamp():
                return

        # Mark Transform as finished
        self.database.mark_collection_store_done(self.destination_collection.database_id)

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
