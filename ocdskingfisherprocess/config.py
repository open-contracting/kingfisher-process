import os
import configparser
import pgpasslib
import sys


"""This holds configuration information for Kingfisher.
Whatever tool is calling it - CLI or other code - should create one of these, set it up as required and pass it around.
"""


class Config:

    def __init__(self):
        self.web_api_keys = []
        self.database_uri = ''
        self._database_host = ''
        self._database_port = 5432
        self._database_user = ''
        self._database_name = ''
        self._database_password = ''
        self.default_value_collection_check_data = False
        self.default_value_collection_check_older_data_with_schema_version_1_1 = False
        self.run_standard_pipeline = False
        self.redis_host = None
        self.redis_port = 6379
        self.redis_database = 0
        self.sentry_dsn = None

    def load_user_config(self):
        # First, try and load any config in the ini files
        self._load_user_config_ini()
        # Second, loook for password in .pggass
        self._load_user_config_pgpass()
        # Third, try and load any config in the env (so env overwrites ini)
        self._load_user_config_env()

    def _load_user_config_pgpass(self):
        if not self._database_name or not self._database_user:
            return

        try:
            password = pgpasslib.getpass(
                self._database_host,
                self._database_port,
                self._database_name,
                self._database_user
            )
            if password:
                self._database_password = password
                self.database_uri = 'postgresql://{}:{}@{}:{}/{}'.format(
                    self._database_user,
                    self._database_password,
                    self._database_host,
                    self._database_port,
                    self._database_name
                )

        except pgpasslib.FileNotFound:
            # Fail silently when no files found.
            return
        except pgpasslib.InvalidPermissions:
            print(
                "Your pgpass file has the wrong permissions, for your safety this file will be ignored. Please fix the permissions and try again.")
            return
        except pgpasslib.PgPassException:
            print("Unexpected error:", sys.exc_info()[0])
            return

    def _load_user_config_env(self):
        if os.environ.get('KINGFISHER_PROCESS_WEB_API_KEYS'):
            self.web_api_keys = [key.strip() for key in os.environ.get('KINGFISHER_PROCESS_WEB_API_KEYS').split(',')]

        if os.environ.get('KINGFISHER_PROCESS_DB_URI'):
            self.database_uri = os.environ.get('KINGFISHER_PROCESS_DB_URI')

    def _load_user_config_ini(self):
        config = configparser.ConfigParser()

        if os.path.isfile(os.path.expanduser('~/.config/ocdskingfisher-process/config.ini')):
            config.read(os.path.expanduser('~/.config/ocdskingfisher-process/config.ini'))
        else:
            return

        self.web_api_keys = [key.strip() for key in config.get('WEB', 'API_KEYS', fallback='').split(',')]

        self._database_host = config.get('DBHOST', 'HOSTNAME')
        self._database_port = config.get('DBHOST', 'PORT')
        self._database_user = config.get('DBHOST', 'USERNAME')
        self._database_name = config.get('DBHOST', 'DBNAME')
        self._database_password = config.get('DBHOST', 'PASSWORD', fallback='')

        self.default_value_collection_check_data = config.getboolean('COLLECTION_DEFAULT', 'CHECK_DATA', fallback=False)
        self.default_value_collection_check_older_data_with_schema_version_1_1 = \
            config.getboolean('COLLECTION_DEFAULT', 'CHECK_OLDER_DATA_WITH_SCHEMA_1_1', fallback=False)

        self.database_uri = 'postgresql://{}:{}@{}:{}/{}'.format(
            self._database_user,
            self._database_password,
            self._database_host,
            self._database_port,
            self._database_name
        )

        self.run_standard_pipeline = \
            config.getboolean('STANDARD_PIPELINE', 'RUN', fallback=False)

        self.redis_host = config.get('REDIS', 'HOST', fallback=None)
        self.redis_port = config.get('REDIS', 'PORT', fallback=6379)
        self.redis_database = config.get('REDIS', 'DATABASE', fallback=0)

        self.sentry_dsn = config.get('SENTRY', 'DSN', fallback=None)

    def is_redis_available(self):
        return self.redis_host and self.redis_port
