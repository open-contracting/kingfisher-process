import hashlib
import os
from textwrap import fill
from urllib.parse import parse_qs, urlencode, urlsplit

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


def get_rabbit_channel(rabbit_exchange_name):
    parsed = urlsplit(settings.RABBIT_URL)
    query = parse_qs(parsed.query)
    query.update({"blocked_connection_timeout": 3600, "heartbeat": 5})

    connection = pika.BlockingConnection(pika.URLParameters(parsed._replace(query=urlencode(query)).geturl()))

    rabbit_channel = connection.channel()
    rabbit_channel.exchange_declare(exchange=rabbit_exchange_name, durable=True, exchange_type="direct")

    return rabbit_channel, connection


def get_env_id():
    return "{}_{}".format(settings.ENV_NAME, settings.ENV_VERSION)
