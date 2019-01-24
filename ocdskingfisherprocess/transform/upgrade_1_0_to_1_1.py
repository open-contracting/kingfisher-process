from ocdskingfisherprocess.transform.base import BaseTransform
from ocdskit.upgrade import upgrade_10_11
import sqlalchemy as sa
import datetime


class Upgrade10To11Transform(BaseTransform):
    type = 'upgrade-1-0-to-1-1'

    def process(self):
        for file_model in self.database.get_all_files_in_collection(self.source_collection.database_id):
            self.process_file(file_model)
            # Early return?
            if self.run_until_timestamp and self.run_until_timestamp < datetime.datetime.utcnow().timestamp():
                return

    def process_file(self, file_model):
        for file_item_model in self.database.get_all_files_items_in_file(file_model):
            self.process_file_item(file_model, file_item_model)
            # Early return?
            if self.run_until_timestamp and self.run_until_timestamp < datetime.datetime.utcnow().timestamp():
                return

    def process_file_item(self, file_model, file_item_model):
        with self.database.get_engine().begin() as connection:
            release_rows = connection.execute(
                self.database.release_table.select().where(
                    self.database.release_table.c.collection_file_item_id == file_item_model.database_id)
            )

        for release_row in release_rows:
            if not self.has_release_id_been_done(release_row['id']):
                self.process_release_row(file_model, file_item_model, release_row)
            # Early return?
            if self.run_until_timestamp and self.run_until_timestamp < datetime.datetime.utcnow().timestamp():
                return

        del release_rows

        with self.database.get_engine().begin() as connection:
            record_rows = connection.execute(
                self.database.record_table.select().where(
                    self.database.record_table.c.collection_file_item_id == file_item_model.database_id)
            )

        for record_row in record_rows:
            if not self.has_record_id_been_done(record_row['id']):
                self.process_record_row(file_model, file_item_model, record_row)
            # Early return?
            if self.run_until_timestamp and self.run_until_timestamp < datetime.datetime.utcnow().timestamp():
                return

    def process_release_row(self, file_model, file_item_model, release_row):
        package = self.database.get_package_data(release_row.package_data_id)
        package['releases'] = [self.database.get_data(release_row.data_id)]
        upgrade_10_11(package)

        self.store.store_file_item(file_model.filename, None, 'release_package', package, file_item_model.number)

        # Doing it like this isn't the best; it means there are 2 DB transactions when really you want one.
        # We'll put this in now and take a note to improve that.
        with self.database.get_engine().begin() as connection:
            connection.execute(self.database.transform_upgrade_1_0_to_1_1_status_release_table.insert(), {
                'source_release_id': release_row.id,
            })

    def process_record_row(self, file_model, file_item_model, record_row):
        package = self.database.get_package_data(record_row.package_data_id)
        package['records'] = [self.database.get_data(record_row.data_id)]
        upgrade_10_11(package)

        self.store.store_file_item(file_model.filename, None, 'record_package', package, file_item_model.number)

        # Doing it like this isn't the best; it means there are 2 DB transactions when really you want one.
        # We'll put this in now and take a note to improve that.
        with self.database.get_engine().begin() as connection:
            connection.execute(self.database.transform_upgrade_1_0_to_1_1_status_record_table.insert(), {
                'source_record_id': record_row.id,
            })

    def has_release_id_been_done(self, release_id):
        with self.database.get_engine().begin() as connection:
            s = sa.sql.select([self.database.transform_upgrade_1_0_to_1_1_status_release_table]) \
                .where(self.database.transform_upgrade_1_0_to_1_1_status_release_table.c.source_release_id == release_id)
            result = connection.execute(s)
            return result.rowcount == 1

    def has_record_id_been_done(self, record_id):
        with self.database.get_engine().begin() as connection:
            s = sa.sql.select([self.database.transform_upgrade_1_0_to_1_1_status_record_table]) \
                .where(self.database.transform_upgrade_1_0_to_1_1_status_record_table.c.source_record_id == record_id)
            result = connection.execute(s)
            return result.rowcount == 1
