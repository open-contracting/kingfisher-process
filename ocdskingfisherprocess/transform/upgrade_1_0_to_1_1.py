import datetime

import sqlalchemy as sa
from ocdskit.upgrade import upgrade_10_11

from ocdskingfisherprocess.database import DatabaseStore
from ocdskingfisherprocess.transform.base import BaseTransform


class Upgrade10To11Transform(BaseTransform):

    def process(self):
        # Is Source Collection still here and not deleted?
        if not self.source_collection or self.source_collection.deleted_at:
            return

        # Is destination deleted?
        if self.destination_collection.deleted_at:
            return

        # Have we already marked this transform as finished?
        if self.destination_collection.store_end_at:
            return

        # Do the work ...
        with self.database.get_engine().connect() as connection:
            release_rows = connection.execute(
                sa.text("""
                    SELECT r.id, filename, url, number, package_data_id, data_id
                    FROM release r
                    INNER JOIN collection_file_item cf ON cf.id = r.collection_file_item_id
                    INNER JOIN collection_file f ON f.id = cf.collection_file_id
                    LEFT JOIN transform_upgrade_1_0_to_1_1_status_release ON r.id = source_release_id
                    WHERE r.collection_id = :collection_id AND source_release_id IS NULL
                """), collection_id=self.source_collection.database_id
            ).fetchall()

        for row in release_rows:
            self.process_row(row, 'release')
            # Early return?
            if self.run_until_timestamp and self.run_until_timestamp < datetime.datetime.utcnow().timestamp():
                return

        with self.database.get_engine().connect() as connection:
            record_rows = connection.execute(
                sa.text("""
                    SELECT r.id, filename, url, number, package_data_id, data_id
                    FROM record r
                    INNER JOIN collection_file_item cf ON cf.id = r.collection_file_item_id
                    INNER JOIN collection_file f ON f.id = cf.collection_file_id
                    LEFT JOIN transform_upgrade_1_0_to_1_1_status_record ON r.id = source_record_id
                    WHERE r.collection_id = :collection_id AND source_record_id IS NULL
                """), collection_id=self.source_collection.database_id
            ).fetchall()

        for row in record_rows:
            self.process_row(row, 'record')
            # Early return?
            if self.run_until_timestamp and self.run_until_timestamp < datetime.datetime.utcnow().timestamp():
                return

        # We maybe mark the transform as finished here
        # There is a race condition we have to be careful off
        # * Collection starts being downloaded, 100 files downloaded and saved
        # * Transform starts
        # * Transform loads list of 100 files into memory
        # * Transform takes a while to process all 100 files
        # * In that time, 10 final files are downloaded and saved and source collection is marked as ended
        # * Now we can't mark the destination collection as closed because we haven't processed the 10 final files!
        # Fortunately, because this check is "if self.source_collection.store_end_at" and
        #   because "self.source_collection" is loaded into memory right at start, this race condition can't occur.
        # But leaving comment as warning to others that order of loading things into memory is important
        if self.source_collection.store_end_at:
            self.database.mark_collection_store_done(self.destination_collection.database_id)

    def process_row(self, row, type):
        self.logger.info(f"upgrade-1-0-to-1-1 to collection {self.destination_collection.database_id} {type} ID {row['id']}")
        package = self.database.get_package_data(row['package_data_id'])
        package[f'{type}s'] = [self.database.get_data(row['data_id'])]
        package = upgrade_10_11(package)

        def add_status(database, connection):
            connection.execute(getattr(database, f'transform_upgrade_1_0_to_1_1_status_{type}_table').insert(), {
                f'source_{type}_id': row['id'],
            })

        package_data = {}
        for key, value in package.items():
            if key != f'{type}s':
                package_data[key] = value

        with DatabaseStore(database=self.database, collection_id=self.destination_collection.database_id,
                           file_name=row['filename'], number=row['number'],
                           url=row['url'], before_db_transaction_ends_callback=add_status,
                           allow_existing_collection_file_item_table_row=True) as store:

            getattr(store, f'insert_{type}')(package[f'{type}s'][0], package_data)
