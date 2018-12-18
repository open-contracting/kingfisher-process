from ocdskingfisherprocess import database
from libcoveocds.api import ocds_json_output, APIException
import sqlalchemy as sa
import tempfile
import shutil


class Checks:

    def __init__(self, database, collection, override_schema_version=None):
        self.database = database
        self.collection = collection
        self.override_schema_version = override_schema_version

    def process_all_files(self):
        for file_model in self.database.get_all_files_in_collection(self.collection.database_id):
            self.process_file(file_model=file_model)

    def process_file(self, file_model):
        for file_item_model in self.database.get_all_files_items_in_file(file_model):
            self.process_file_item(file_item_model=file_item_model)

    def process_file_item(self, file_item_model):
        with self.database.get_engine().begin() as connection:
            release_rows = connection.execute(
                self.database.release_table.select().where(self.database.release_table.c.collection_file_item_id == file_item_model.database_id)
            )

        for release_row in release_rows:
            if not self.database.is_release_check_done(release_row['id'], override_schema_version=self.override_schema_version):
                self.check_release_row(release_row, override_schema_version=self.override_schema_version)

        del release_rows

        with self.database.get_engine().begin() as connection:
            record_rows = connection.execute(
                self.database.record_table.select().where(self.database.record_table.c.collection_file_item_id == file_item_model.database_id)
            )

        for record_row in record_rows:
            if not self.database.is_record_check_done(record_row['id'], override_schema_version=self.override_schema_version):
                self.check_record_row(record_row, override_schema_version=self.override_schema_version)

    def handle_package(self, package):
        cove_temp_folder = tempfile.mkdtemp(prefix='ocdskingfisher-cove-', dir=tempfile.gettempdir())
        try:
            return ocds_json_output(cove_temp_folder, None, None, convert=False, cache_schema=True, file_type='json', json_data=package)
        finally:
            shutil.rmtree(cove_temp_folder)

    def get_package_data(self, package_data_id):
        with self.database.get_engine().begin() as connection:
            s = sa.sql.select([self.database.package_data_table]) \
                .where(self.database.package_data_table.c.id == package_data_id)
            result = connection.execute(s)
            data_row = result.fetchone()
            return data_row['data']

    def get_data(self, data_id):
        with self.database.get_engine().begin() as connection:
            s = sa.sql.select([self.database.data_table]) \
                .where(self.database.data_table.c.id == data_id)
            result = connection.execute(s)
            data_row = result.fetchone()
            return data_row['data']

    def check_release_row(self, release_row, override_schema_version=None):
        package = self.get_package_data(release_row.package_data_id)
        package['releases'] = [self.get_data(release_row.data_id)]
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
                connection.execute(database.release_check_error_table.insert(), checks)

    def check_record_row(self, record_row, override_schema_version=None):
        package = self.get_package_data(record_row.package_data_id)
        package['records'] = [self.get_data(record_row.data_id)]
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
                connection.execute(database.record_check_table.insert(), checks)
        except APIException as err:
            checks = [{
                'record_id': record_row.id,
                'error': str(err),
                'override_schema_version': override_schema_version
            }]
            with self.database.get_engine().begin() as connection:
                connection.execute(database.record_check_error_table.insert(), checks)
