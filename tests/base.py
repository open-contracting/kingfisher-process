from ocdskingfisherprocess.database import DataBase
from ocdskingfisherprocess.config import Config
from ocdskingfisherprocess.web.app import create_app
from ocdskingfisherprocess.signals import KINGFISHER_SIGNALS
from ocdskingfisherprocess.signals.signals import setup_signals


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


class BaseDataBaseTest(AbstractDataBaseTest):
    def setup_method(self, test_method):
        super().setup_method(test_method)

        _reset_signals()
        setup_signals(self.config, self.database)


class BaseWebTest(AbstractDataBaseTest):
    def setup_method(self, test_method):
        super().setup_method(test_method)

        _reset_signals()
        # don't call - setup_signals(self.config, self.database) - create_app will

        self.webapp = create_app(config=self.config)
        self.webapp.config['TESTING'] = True
        self.flaskclient = self.webapp.test_client()
