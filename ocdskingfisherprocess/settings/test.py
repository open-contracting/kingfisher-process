import os

from .base import *  # noqa: F403,F401

ENV_NAME = "test"
ENV_VERSION = "1.0"

# Build paths inside the project like this: os.path.join(BASE_DIR, ...)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


# Quick-start development settings - unsuitable for production
# See https://docs.djangoproject.com/en/3.0/howto/deployment/checklist/

# SECURITY WARNING: keep the secret key used in production secret!
SECRET_KEY = os.getenv('SECRET_KEY', '^0z5u6!dqjb%7s4&3nhg57q-h%)+_u*osk5k!uf-6n_0#2*p_4')

# SECURITY WARNING: don't run with debug turned on in production!
DEBUG = True

ALLOWED_HOSTS = ['localhost', '127.0.0.1']

RABBIT_URL = ""

# The schema in the older version had index names longer than 30 characters.
SILENCED_SYSTEM_CHECKS = [
    'models.E034',
]

LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "console": {
            "format": "%(asctime)s %(levelname)s [%(name)s:%(lineno)s] %(message)s",
        },
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "console",
        },
    },
    "loggers": {
        "": {
            "handlers": ["console"],
            "level": "INFO",
        },
        "process": {
            "handlers": ["console"],
            "level": "DEBUG",
        },
    },
}