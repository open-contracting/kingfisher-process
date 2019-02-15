from ocdskingfisherprocess.database import DataBase
from ocdskingfisherprocess.config import Config
from ocdskingfisherprocess.web.app import create_app
from ocdskingfisherprocess.signals import KINGFISHER_SIGNALS
from ocdskingfisherprocess.signals.signals import setup_signals


def _reset_signals():
    KINGFISHER_SIGNALS.signal('new_collection_created')._clear_state()


class BaseTest:

    def alter_config(self):
        pass

    def setup_method(self, test_method):
        # config
        self.config = Config()
        self.config.load_user_config()
        self.alter_config()


class BaseDataBaseTest:

    def alter_config(self):
        pass

    def setup_method(self, test_method):
        # config
        self.config = Config()
        self.config.load_user_config()
        self.alter_config()
        # database
        self.database = DataBase(config=self.config)
        self.database.delete_tables()
        self.database.create_tables()
        # signals
        _reset_signals()
        setup_signals(self.config, self.database)


class BaseWebTest:

    def alter_config(self):
        pass

    def setup_method(self, test_method):
        # config
        self.config = Config()
        self.config.load_user_config()
        self.alter_config()
        # database
        self.database = DataBase(config=self.config)
        self.database.delete_tables()
        self.database.create_tables()
        # signals
        _reset_signals()
        # don't call - setup_signals(self.config, self.database) - create_app will
        # flask app
        self.webapp = create_app(config=self.config)
        self.webapp.config['TESTING'] = True
        self.flaskclient = self.webapp.test_client()
