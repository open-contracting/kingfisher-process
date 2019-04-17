import tempfile
import shutil
import datetime
import logging

from libcoveocds.config import LibCoveOCDSConfig
from libcoveocds.api import ocds_json_output, APIException


class Checks:

    def __init__(self, database, collection, run_until_timestamp=None):
        self.database = database
        self.collection = collection
        self.run_until_timestamp = run_until_timestamp
        self.logger = logging.getLogger('ocdskingfisher.checks')
        self.libcoveocds_config = LibCoveOCDSConfig()
        self.libcoveocds_config.config['cache_all_requests'] = True

    def process_all_files(self):

        self.logger.info('process_all_files called for collection ' + str(self.collection.database_id))

        # Is deleted?
        if self.collection.deleted_at:
            return

        # Normal Checks
        if self.collection.check_data:

            self.process_all_files_releases()

            # Early return?
            if self.run_until_timestamp and self.run_until_timestamp < datetime.datetime.utcnow().timestamp():
                return

            self.process_all_files_records()

            # Early return?
            if self.run_until_timestamp and self.run_until_timestamp < datetime.datetime.utcnow().timestamp():
                return

        # Checks with schema V1.1
        if self.collection.check_older_data_with_schema_version_1_1:

            self.process_all_files_releases_with_override_schema_version_1_1()

            # Early return?
            if self.run_until_timestamp and self.run_until_timestamp < datetime.datetime.utcnow().timestamp():
                return

            self.process_all_files_records_with_override_schema_version_1_1()

    def process_all_files_releases(self):
        results = self.database.get_releases_to_check(self.collection.database_id)
        for release_row in results:
            # Do Normal Check?
            if not self.database.is_release_check_done(release_row['id']):
                self.check_release_row(release_row)
            # Early return?
            if self.run_until_timestamp and self.run_until_timestamp < datetime.datetime.utcnow().timestamp():
                return

    def process_all_files_records(self):
        results = self.database.get_records_to_check(self.collection.database_id)
        for record_row in results:
            # Do Normal Check?
            if not self.database.is_record_check_done(record_row['id']):
                self.check_record_row(record_row)
            # Early return?
            if self.run_until_timestamp and self.run_until_timestamp < datetime.datetime.utcnow().timestamp():
                return

    def process_all_files_releases_with_override_schema_version_1_1(self):
        results = self.database.get_releases_to_check(self.collection.database_id, override_schema_version="1.1")
        for release_row in results:
            # Do 1.1 check?
            if self.is_schema_version_less_than_1_1(release_row['package_data_id']) \
                    and not self.database.is_release_check_done(release_row['id'], override_schema_version="1.1"):
                self.check_release_row(release_row, override_schema_version="1.1")
            # Early return?
            if self.run_until_timestamp and self.run_until_timestamp < datetime.datetime.utcnow().timestamp():
                return

    def process_all_files_records_with_override_schema_version_1_1(self):
        results = self.database.get_records_to_check(self.collection.database_id, override_schema_version="1.1")
        for record_row in results:
            # Do 1.1 check?
            if self.is_schema_version_less_than_1_1(record_row['package_data_id']) \
                    and not self.database.is_record_check_done(record_row['id'], override_schema_version="1.1"):
                self.check_record_row(record_row, override_schema_version="1.1")
            # Early return?
            if self.run_until_timestamp and self.run_until_timestamp < datetime.datetime.utcnow().timestamp():
                return

    def handle_package(self, package):
        cove_temp_folder = tempfile.mkdtemp(prefix='ocdskingfisher-cove-', dir=tempfile.gettempdir())
        try:
            return ocds_json_output(cove_temp_folder, None, None,
                                    convert=False,
                                    lib_cove_ocds_config=self.libcoveocds_config,
                                    file_type='json',
                                    json_data=package)
        finally:
            shutil.rmtree(cove_temp_folder)

    def is_schema_version_less_than_1_1(self, package_data_id):
        # Performance wise, this is a bit dumb. We are basically calling get_package_data twice in a row!
        # We trust the database server will cache the result.
        # Later, maybe we'll want to see if improving this makes a big difference.
        data = self.database.get_package_data(package_data_id)
        return 'version' not in data or data['version'] == "1.0"

    def check_release_row(self, release_row, override_schema_version=None):
        self.logger.debug('check_release_row called for row ' + str(release_row.id) + ' in collection ' + str(self.collection.database_id))
        package = self.database.get_package_data(release_row.package_data_id)
        package['releases'] = [self.database.get_data(release_row.data_id)]
        if override_schema_version:
            package['version'] = override_schema_version
        try:
            cove_output = self.handle_package(package)
            checks = [{
                'release_id': release_row.id,
                'cove_output': cove_output,
                'override_schema_version': override_schema_version
            }]
            with self.database.get_engine().begin() as connection:
                connection.execute(self.database.release_check_table.insert(), checks)
        except APIException as err:
            checks = [{
                'release_id': release_row.id,
                'error': str(err),
                'override_schema_version': override_schema_version
            }]
            with self.database.get_engine().begin() as connection:
                connection.execute(self.database.release_check_error_table.insert(), checks)

    def check_record_row(self, record_row, override_schema_version=None):
        self.logger.debug('check_record_row called for row ' + str(record_row.id) + ' in collection ' + str(self.collection.database_id))
        package = self.database.get_package_data(record_row.package_data_id)
        package['records'] = [self.database.get_data(record_row.data_id)]
        if override_schema_version:
            package['version'] = override_schema_version
        try:
            cove_output = self.handle_package(package)
            checks = [{
                'record_id': record_row.id,
                'cove_output': cove_output,
                'override_schema_version': override_schema_version
            }]
            with self.database.get_engine().begin() as connection:
                connection.execute(self.database.record_check_table.insert(), checks)
        except APIException as err:
            checks = [{
                'record_id': record_row.id,
                'error': str(err),
                'override_schema_version': override_schema_version
            }]
            with self.database.get_engine().begin() as connection:
                connection.execute(self.database.record_check_error_table.insert(), checks)
