import hashlib
import os
from textwrap import fill

import orjson
import pika
from django.conf import settings


def json_dumps(data):
    """
    Dumps JSON to a string, and returns it.
    """
    return orjson.dumps(data)


def wrap(string):
    """
    Formats a long string as a help message, and returns it.
    """
    return "\n\n".join(fill(paragraph, width=78, replace_whitespace=False) for paragraph in string.splitlines())


def walk(paths):
    for path in paths:
        if os.path.isfile(path):
            yield path
        else:
            for root, dirs, files in os.walk(path):
                for name in files:
                    if not name.startswith("."):
                        yield os.path.join(root, name)


def get_hash(data):
    return hashlib.md5(data.encode("utf-8")).hexdigest()


def get_rabbit_channel(rabbit_exchange):
    # connect to messaging
    credentials = pika.PlainCredentials(settings.RABBITMQ["username"], settings.RABBITMQ["password"])

    connection = pika.BlockingConnection(
        pika.ConnectionParameters(
            host=settings.RABBITMQ["host"],
            port=settings.RABBITMQ["port"],
            credentials=credentials,
            blocked_connection_timeout=1800,
            heartbeat=0,
        )
    )

    rabbit_channel = connection.channel()

    # declare durable exchange
    rabbit_channel.exchange_declare(exchange=rabbit_exchange, durable="true", exchange_type="direct")

    return rabbit_channel


def get_env_id():
    return "{}_{}".format(settings.ENV_NAME, settings.ENV_VERSION)
