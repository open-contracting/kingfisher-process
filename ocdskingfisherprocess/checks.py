from libcoveocds.api import ocds_json_output, APIException
import tempfile
import shutil
import datetime


class Checks:

    def __init__(self, database, collection, run_until_timestamp=None):
        self.database = database
        self.collection = collection
        self.run_until_timestamp = run_until_timestamp

    def process_all_files(self):
        if not self.collection.check_data and not self.collection.check_older_data_with_schema_version_1_1:
            # nothing to do here, so ...
            return

        for file_model in self.database.get_all_files_in_collection(self.collection.database_id):
            self.process_file(file_model=file_model)
            # Early return?
            if self.run_until_timestamp and self.run_until_timestamp < datetime.datetime.utcnow().timestamp():
                return

    def process_file(self, file_model):
        for file_item_model in self.database.get_all_files_items_in_file(file_model):
            self.process_file_item(file_item_model=file_item_model)
            # Early return?
            if self.run_until_timestamp and self.run_until_timestamp < datetime.datetime.utcnow().timestamp():
                return

    def process_file_item(self, file_item_model):
        with self.database.get_engine().begin() as connection:
            release_rows = connection.execute(
                self.database.release_table.select().where(self.database.release_table.c.collection_file_item_id == file_item_model.database_id)
            )

        for release_row in release_rows:
            # Do Normal Check?
            if self.collection.check_data and not self.database.is_release_check_done(release_row['id']):
                self.check_release_row(release_row)
            # Do 1.1 check?
            if self.collection.check_older_data_with_schema_version_1_1 \
                    and self.is_schema_version_less_than_1_1(release_row['package_data_id']) \
                    and not self.database.is_release_check_done(release_row['id'], override_schema_version="1.1"):
                self.check_release_row(release_row, override_schema_version="1.1")
            # Early return?
            if self.run_until_timestamp and self.run_until_timestamp < datetime.datetime.utcnow().timestamp():
                return

        del release_rows

        with self.database.get_engine().begin() as connection:
            record_rows = connection.execute(
                self.database.record_table.select().where(self.database.record_table.c.collection_file_item_id == file_item_model.database_id)
            )

        for record_row in record_rows:
            # Do Normal Check?
            if self.collection.check_data and not self.database.is_record_check_done(record_row['id']):
                self.check_record_row(record_row)
            # Do 1.1 check?
            if self.collection.check_older_data_with_schema_version_1_1 \
                    and self.is_schema_version_less_than_1_1(record_row['package_data_id']) \
                    and not self.database.is_record_check_done(record_row['id'], override_schema_version="1.1"):
                self.check_record_row(record_row, override_schema_version="1.1")
            # Early return?
            if self.run_until_timestamp and self.run_until_timestamp < datetime.datetime.utcnow().timestamp():
                return

    def handle_package(self, package):
        cove_temp_folder = tempfile.mkdtemp(prefix='ocdskingfisher-cove-', dir=tempfile.gettempdir())
        try:
            return ocds_json_output(cove_temp_folder, None, None, convert=False, cache_schema=True, file_type='json', json_data=package)
        finally:
            shutil.rmtree(cove_temp_folder)

    def is_schema_version_less_than_1_1(self, package_data_id):
        # Performance wise, this is a bit dumb. We are basically calling get_package_data twice in a row!
        # We trust the database server will cache the result.
        # Later, maybe we'll want to see if improving this makes a big difference.
        data = self.database.get_package_data(package_data_id)
        return 'version' not in data or data['version'] == "1.0"

    def check_release_row(self, release_row, override_schema_version=None):
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
