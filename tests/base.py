from ocdskingfisherprocess.database import DataBase
from ocdskingfisherprocess.config import Config
from ocdskingfisherprocess.web.app import create_app


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
        # flask app
        self.webapp = create_app(config=self.config)
        self.webapp.config['TESTING'] = True
        self.flaskclient = self.webapp.test_client()
