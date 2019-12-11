import os
from datetime import datetime

import sqlalchemy as sa

from ocdskingfisherprocess.config import Config
from ocdskingfisherprocess.database import DataBase
from ocdskingfisherprocess.signals import KINGFISHER_SIGNALS
from ocdskingfisherprocess.signals.signals import setup_signals
from ocdskingfisherprocess.store import Store
from ocdskingfisherprocess.web.app import create_app


def _reset_signals():
    KINGFISHER_SIGNALS.signal('new_collection_created')._clear_state()


class BaseTest:
    def alter_config(self):
        """
        Implement this method in a subclass to edit ``self.config``.
        """
        pass

    # https://docs.pytest.org/en/latest/xunit_setup.html#method-and-function-level-setup-teardown
    def setup_method(self, test_method):
        self.config = Config()
        self.config.load_user_config()
        self.alter_config()


class AbstractDataBaseTest(BaseTest):
    def setup_method(self, test_method):
        super().setup_method(test_method)

        self.database = DataBase(config=self.config)
        self.database.delete_tables()
        self.database.create_tables()

    def assert_row_counts(self, connection, counts):
        """
        Asserts that the tables contain the numbers of rows.
        """
        for table, count in counts.items():
            self.assert_row_count(connection, table, count)

    def assert_row_count(self, connection, table, count):
        """
        Asserts that the table contains the number of rows, and returns the result.
        """
        s = sa.sql.select([getattr(self.database, table + '_table')])
        result = connection.execute(s)
        assert result.rowcount == count

        return result


class BaseDataBaseTest(AbstractDataBaseTest):
    """
    The base class for all tests that interact with the database.

    To setup the collection, implement a ``setup_collection`` instance method that accepts the collection ID as its
    only argument.
    """

    def setup_method(self, test_method):
        super().setup_method(test_method)

        _reset_signals()
        setup_signals(self.config, self.database)

    def get_collection_and_store_file(self, basename, data_type):
        """
        Creates and configures a collection, stores a file, and returns the collection ID and collection.
        """
        collection_id = self.database.get_or_create_collection_id('test', datetime.now(), False)
        self.setup_collection(collection_id)

        collection = self.database.get_collection(collection_id)
        store = Store(self.config, self.database)
        store.set_collection(collection)

        filename = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'data', basename)
        store.store_file_from_local('test.json', 'http://example.com', data_type, 'utf-8', filename)

        return collection_id, collection

    def get_transformed_collection(self, source_collection_id, source_collection, transform_type):
        """
        Creates a transformed collection, and returns the collection ID and collection.
        """
        collection_id = self.database.get_or_create_collection_id(
            source_collection.source_id,
            source_collection.data_version,
            source_collection.sample,
            transform_from_collection_id=source_collection_id,
            transform_type=transform_type)

        collection = self.database.get_collection(collection_id)

        return collection_id, collection

    def setup_collection(self, collection_id):
        """
        Implement this method in a subclass to change the collection's configuration.
        """
        pass


class BaseWebTest(AbstractDataBaseTest):
    def setup_method(self, test_method):
        super().setup_method(test_method)

        _reset_signals()
        # don't call - setup_signals(self.config, self.database) - create_app will

        self.webapp = create_app(config=self.config)
        self.webapp.config['TESTING'] = True
        self.flaskclient = self.webapp.test_client()
