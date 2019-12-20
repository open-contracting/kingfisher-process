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
            self.process_ocid(ocid)
            # Early return?
            if self.run_until_timestamp and self.run_until_timestamp < datetime.datetime.utcnow().timestamp():
                return

        # Mark Transform as finished
        self.database.mark_collection_store_done(self.destination_collection.database_id)

    def get_ocids(self):
        ''' Gets the ocids for this collection that have not been transformed'''
        ocids = []

        with self.database.get_engine().begin() as engine:
            query = engine.execute(
                sa.text(
                    " SELECT r.ocid FROM release_with_collection AS r" +
                    " LEFT JOIN compiled_release_with_collection AS cr ON " +
                    " cr.ocid = r.ocid and cr.collection_id = :destination_collection_id" +
                    " WHERE r.collection_id = :collection_id and cr.ocid is NULL" +
                    " GROUP BY r.ocid "
                ),
                collection_id=self.source_collection.database_id,
                destination_collection_id=self.destination_collection.database_id
            )

            for row in query:
                ocids.append(row['ocid'])

        return ocids

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

        # Are any releases already compiled? https://github.com/open-contracting/kingfisher-process/issues/147
        releases_compiled = \
            [x for x in releases if 'tag' in x and isinstance(x['tag'], list) and 'compiled' in x['tag']]

        if len(releases_compiled) > 1:
            # If more than one, pick one at random. and log that.
            warning = 'This already has multiple compiled releases in the source! ' +\
                      'We have picked one at random and passed it through this transform unchanged.'
            self.store.store_file_item(
                ocid + '.json',
                '',
                'compiled_release',
                releases_compiled[0],
                1,
                warnings=[warning])
        elif len(releases_compiled) == 1:
            # There is just one compiled release - pass it through unchanged, and log that.
            warning = 'This already has one compiled release in the source! ' +\
                      'We have passed it through this transform unchanged.'
            self.store.store_file_item(
                ocid + '.json',
                '',
                'compiled_release',
                releases_compiled[0],
                1,
                warnings=[warning])
        else:
            # There is no compiled release - we will do it ourselves.
            out = ocdsmerge.merge(releases)

            # In the occurrence of a race condition where two concurrent transforms have run the same ocid
            # we rely on the fact that collection_id and filename are unique in the file_item table.
            # Therefore this will error with a violation of unique key contraint and not cause duplicate entries.
            self.store.store_file_item(ocid+'.json', '', 'compiled_release', out, 1)
