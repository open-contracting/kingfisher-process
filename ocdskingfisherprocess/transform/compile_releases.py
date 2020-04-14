import datetime

import ocdsmerge
import sqlalchemy as sa

from ocdskingfisherprocess.transform.base import BaseTransform


class CompileReleasesTransform(BaseTransform):

    def process(self):
        # Is Source Collection still here and not deleted?
        if not self.source_collection or self.source_collection.deleted_at:
            return

        # Is destination deleted?
        if self.destination_collection.deleted_at:
            return

        # This transform can only run when the source collection is fully stored!
        if not self.source_collection.store_end_at:
            return

        # Have we already marked this transform as finished?
        if self.destination_collection.store_end_at:
            return

        # Do the work ...
        for ocid in self._get_ocids():
            self._process_ocid(ocid)
            # Early return?
            if self.run_until_timestamp and self.run_until_timestamp < datetime.datetime.utcnow().timestamp():
                return

        # Mark Transform as finished
        self.database.mark_collection_store_done(self.destination_collection.database_id)

    def _get_ocids(self):
        ''' Gets the ocids for this collection that have not been transformed'''
        ocids = []

        # get ocids from releases
        with self.database.get_engine().begin() as engine:
            query = engine.execute(
                sa.text(
                    " SELECT r.ocid FROM release AS r" +
                    " LEFT JOIN compiled_release AS cr ON " +
                    " cr.ocid = r.ocid and cr.collection_id = :destination_collection_id" +
                    " WHERE r.collection_id = :collection_id and cr.ocid is NULL" +
                    " GROUP BY r.ocid "
                ),
                collection_id=self.source_collection.database_id,
                destination_collection_id=self.destination_collection.database_id
            )

            for row in query:
                ocids.append(row['ocid'])

        # get ocids from records
        with self.database.get_engine().begin() as engine:
            query = engine.execute(
                sa.text(
                    " SELECT r.ocid FROM record AS r" +
                    " LEFT JOIN compiled_release AS cr ON " +
                    " cr.ocid = r.ocid and cr.collection_id = :destination_collection_id" +
                    " WHERE r.collection_id = :collection_id and cr.ocid is NULL" +
                    " GROUP BY r.ocid "
                ),
                collection_id=self.source_collection.database_id,
                destination_collection_id=self.destination_collection.database_id
            )

            for row in query:
                if row['ocid'] not in ocids:
                    ocids.append(row['ocid'])

        # done
        return ocids

    def _process_ocid(self, ocid):

        # Load Records
        records = []
        with self.database.get_engine().begin() as engine:
            query = engine.execute(sa.text(
                " SELECT record.* FROM record " +
                " WHERE record.collection_id = :collection_id AND record.ocid = :ocid "
            ), collection_id=self.source_collection.database_id, ocid=ocid)

            for row in query:
                records.append(self.database.get_data(row['data_id']))

        # Decide what to do .....
        if len(records) > 1:

            # This counts as taking a random record, as we have not ordered the SQL query by anything.
            # (In practice the way postgres works I think we will always get the first record by load order,
            #     but with no ORDER BY clause that is not guaranteed)
            # TODO log we have done this
            self._process_record(ocid, records[0])

        elif len(records) == 1:

            self._process_record(ocid, records[0])

        else:

            self._process_releases(ocid)

    def _process_record(self, ocid, record):

        releases = record.get('releases', [])
        releases_linked = [r for r in releases if 'url' in r and r['url']]

        if len(releases) > 0 and len(releases_linked) == 0:
            # We have releases and none are linked (have URL's).
            # We can compile them ourselves.
            merger = ocdsmerge.Merger()
            out = merger.create_compiled_release(releases)
            self._store_result(ocid, out)
            return

        compiled_release = record.get('compiledRelease')
        if compiled_release:

            # TODO log we have done this

            self._store_result(ocid, compiled_release)
            return

        releases_compiled = \
            [x for x in releases if 'tag' in x and isinstance(x['tag'], list) and 'compiled' in x['tag']]

        if len(releases_compiled) > 1:
            # If more than one, pick one at random. and log that.
            warning = 'This already has multiple compiled releases in the source! ' + \
                      'We have picked one at random and passed it through this transform unchanged.'
            self._store_result(ocid, releases_compiled[0], warnings=[warning])

        elif len(releases_compiled) == 1:
            # There is just one compiled release - pass it through unchanged, and log that.
            warning = 'This already has one compiled release in the source! ' + \
                      'We have passed it through this transform unchanged.'
            self._store_result(ocid, releases_compiled[0], warnings=[warning])

        else:
            # We can't process this ocid. Warn of that.
            pass
            # TODO log we have done this

    def _process_releases(self, ocid):

        releases = []

        with self.database.get_engine().begin() as engine:
            query = engine.execute(sa.text(
                " SELECT release.* FROM release " +
                " WHERE release.collection_id = :collection_id AND release.ocid = :ocid "
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
            self._store_result(ocid, releases_compiled[0], warnings=[warning])
        elif len(releases_compiled) == 1:
            # There is just one compiled release - pass it through unchanged, and log that.
            warning = 'This already has one compiled release in the source! ' +\
                      'We have passed it through this transform unchanged.'
            self._store_result(ocid, releases_compiled[0], warnings=[warning])
        else:
            # There is no compiled release - we will do it ourselves.
            merger = ocdsmerge.Merger()
            out = merger.create_compiled_release(releases)

            self._store_result(ocid, out)

    def _store_result(self, ocid, data, warnings=[]):
        # In the occurrence of a race condition where two concurrent transforms have run the same ocid
        # we rely on the fact that collection_id and filename are unique in the file_item table.
        # Therefore this will error with a violation of unique key constraint and not cause duplicate entries.
        self.store.store_file_item(ocid+'.json', '', 'compiled_release', data, 1, warnings=warnings)
