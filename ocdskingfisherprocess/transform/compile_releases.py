import datetime

import ocdsmerge
import sqlalchemy as sa
from ocdskit.util import is_linked_release

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
            self.logger.info(f'compile-releases to collection {self.destination_collection.database_id} OCID {ocid}')
            self._process_ocid(ocid)
            # Early return?
            if self.run_until_timestamp and self.run_until_timestamp < datetime.datetime.utcnow().timestamp():
                return

        # Mark Transform as finished
        self.database.mark_collection_store_done(self.destination_collection.database_id)

    def _get_ocids(self):
        ''' Gets the ocids for this collection that have not been transformed'''
        ocids = set()

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

            ocids.update(row['ocid'] for row in query)

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

            ocids.update(row['ocid'] for row in query)

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

            records = [self.database.get_data(row['data_id']) for row in query]

        # Decide what to do .....
        if len(records) > 1:

            warning = 'There are multiple records for this OCID! ' + \
                    'The record to pass through was selected arbitrarily.'
            self._process_record(ocid, records[0], warnings=[warning])

        elif len(records) == 1:

            self._process_record(ocid, records[0])

        else:

            self._process_releases(ocid)

    def _process_record(self, ocid, record, warnings=None):

        if not warnings:
            warnings = []

        releases = record.get('releases', [])
        releases_with_date, releases_without_date = self._check_dates_in_releases(releases)

        # Can we compile ourselves?
        releases_linked = [r for r in releases_with_date if is_linked_release(r)]
        if releases_with_date and not releases_linked:
            # We have releases with date fields and none are linked (have URL's).
            # We can compile them ourselves.
            # (Checking releases_with_date here and not releases means that a record with
            #  a compiledRelease and releases with no dates will be processed by using the compiledRelease,
            #  so we still have some data)
            if releases_without_date:
                warnings.append('This OCID had some releases without a date element. ' +
                                'We have compiled all other releases.')

            self._compile_releases_by_ocdsmerge(ocid, releases_with_date, warnings=warnings)
            return

        # Whatever happens now, users will appreciate a warning about the bad data
        if releases_without_date:
            warnings.append('This OCID had some releases without a date element.')

        # Is there a compiledRelease?
        compiled_release = record.get('compiledRelease')
        if compiled_release:

            warnings.append('This already had a compiledRelease in the record! ' +
                            'It was passed through this transform unchanged.')
            self._store_result(ocid, compiled_release, warnings=warnings)
            return

        # Is there a release tagged 'compiled'?
        releases_compiled = \
            [x for x in releases if 'tag' in x and isinstance(x['tag'], list) and 'compiled' in x['tag']]

        if len(releases_compiled) > 1:
            # If more than one, pick one at random. and log that.
            warnings.append('This already has multiple compiled releases in the releases array! ' +
                            'The compiled release to pass through was selected arbitrarily.')
            self._store_result(ocid, releases_compiled[0], warnings=warnings)

        elif len(releases_compiled) == 1:
            # There is just one compiled release - pass it through unchanged, and log that.
            warnings.append('This already has one compiled release in the releases array! ' +
                            'It was passed through this transform unchanged.')
            self._store_result(ocid, releases_compiled[0], warnings=warnings)

        else:
            # We can't process this ocid. Warn of that.
            self.database.add_collection_note(
                self.destination_collection.database_id,
                'OCID ' + ocid + ' could not be compiled because at least one release in the releases array is a ' +
                'linked release or there are no releases with dates, ' +
                'and the record has neither a compileRelease nor a release with a tag of "compiled".'
            )

    def _process_releases(self, ocid):

        releases = []

        with self.database.get_engine().begin() as engine:
            query = engine.execute(sa.text(
                " SELECT release.* FROM release " +
                " WHERE release.collection_id = :collection_id AND release.ocid = :ocid "
            ), collection_id=self.source_collection.database_id, ocid=ocid)

            releases = [self.database.get_data(row['data_id']) for row in query]

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
            # There is no compiled release - we will try to do it ourselves.
            releases_with_date, releases_without_date = self._check_dates_in_releases(releases)
            if releases_with_date:

                warnings = []
                if releases_without_date:
                    warnings.append('This OCID had some releases without a date element. ' +
                                    'We have compiled all other releases.')

                self._compile_releases_by_ocdsmerge(ocid, releases_with_date, warnings=warnings)
            else:
                # We can't process this ocid. Warn of that.
                self.database.add_collection_note(
                    self.destination_collection.database_id,
                    'OCID ' + ocid + ' could not be compiled because there are no releases with dates ' +
                    'nor a release with a tag of "compiled".'
                )

    def _check_dates_in_releases(self, releases):
        releases_with_date = [r for r in releases if 'date' in r]
        releases_without_date = [r for r in releases if 'date' not in r]
        return releases_with_date, releases_without_date

    def _compile_releases_by_ocdsmerge(self, ocid, releases, warnings=None):
        try:
            merger = ocdsmerge.Merger()
            out = merger.create_compiled_release(releases)
            self._store_result(ocid, out, warnings=warnings)
        except ocdsmerge.exceptions.OCDSMergeError as error:
            self.database.add_collection_note(
                self.destination_collection.database_id,
                'OCID ' + ocid + ' could not be compiled because merge library threw an error: '
                + error.__class__.__name__ + ' ' + str(error)
            )

    def _store_result(self, ocid, data, warnings=None):

        # In the occurrence of a race condition where two concurrent transforms have run the same ocid
        # we rely on the fact that collection_id and filename are unique in the file_item table.
        # Therefore this will error with a violation of unique key constraint and not cause duplicate entries.
        self.store.store_file_item(ocid+'.json', '', 'compiled_release', data, 1, warnings=warnings)
