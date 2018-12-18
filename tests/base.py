from ocdskingfisherprocess.database import DataBase
from ocdskingfisherprocess.config import Config


class BaseTest:

    def setup_method(self, test_method):
        self.config = Config()
        self.config.load_user_config()
        self.database = DataBase(config=self.config)

    def setup_main_database(self):
        self.database.delete_tables()
        self.database.create_tables()
