import hashlib
import logging
import os
import signal
from contextlib import contextmanager
from textwrap import fill

import pika.exceptions
from django.conf import settings
from django.db import connections
from yapw import clients
from yapw.decorators import decorate
from yapw.methods.blocking import nack

from process.exceptions import AlreadyExists
from process.models import CollectionNote, ProcessingStep

logger = logging.getLogger(__name__)


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


class Consumer(clients.Threaded, clients.Durable, clients.Blocking, clients.Base):
    pass


class Publisher(clients.Durable, clients.Blocking, clients.Base):
    pass


def get_client(klass, **kwargs):
    return klass(url=settings.RABBIT_URL, exchange=settings.RABBIT_EXCHANGE_NAME, **kwargs)


@contextmanager
def get_publisher(prefetch_count=1):
    client = get_client(Publisher, prefetch_count=prefetch_count)
    try:
        yield client
    finally:
        client.close()


# https://github.com/pika/pika/blob/master/examples/blocking_consume_recover_multiple_hosts.py
def consume(*args, prefetch_count=1, **kwargs):
    while True:
        try:
            client = get_client(Consumer, prefetch_count=prefetch_count)
            client.consume(*args, **kwargs)
            break
        # Do not recover if the connection was closed by the broker.
        except pika.exceptions.ConnectionClosedByBroker as e:  # subclass of AMQPConnectionError
            logger.warning(e)
            break
        # Recover from "Connection reset by peer".
        except pika.exceptions.StreamLostError as e:  # subclass of AMQPConnectionError
            logger.warning(e)
            continue


def decorator(decode, callback, state, channel, method, properties, body):
    """
    If the callback raises an exception, send the SIGUSR1 signal to the main thread, without acknowledgment. If the
    exception is `AlreadyExists`, assume the same message was delivered twice, log an error, and ack the message.

    Close the database connections opened by the callback, before returning.
    """

    def errback(exception):
        if isinstance(exception, AlreadyExists):
            # The frequency of these errors should be monitored via Sentry. They should be rare. If they are common,
            # then either there is an error in the code or an issue with the connection (heartbeat, etc.).
            logger.error(f"AlreadyExists error possibly caused by duplicate message: {exception}")
            nack(state, channel, method.delivery_tag, requeue=False)
        else:
            logger.exception("Unhandled exception when consuming %r, sending SIGUSR1", body)
            os.kill(os.getpid(), signal.SIGUSR1)

    def finalback():
        for conn in connections.all():
            conn.close()

    decorate(decode, callback, state, channel, method, properties, body, errback, finalback)


def save_note(collection, code, note):
    """Shortcut to save note to collection"""
    collection_note = CollectionNote()
    collection_note.collection = collection
    collection_note.code = code
    collection_note.note = note
    collection_note.save()


def create_step(name, collection_id, **kwargs):
    processing_step = ProcessingStep(name=name, collection_id=collection_id, **kwargs)
    processing_step.save()


def delete_step(step_type=None, collection_id=None, collection_file_id=None, ocid=None):
    processing_steps = ProcessingStep.objects.all()

    if collection_file_id:
        processing_steps = processing_steps.filter(collection_file_id=collection_file_id)

    if ocid:
        processing_steps = processing_steps.filter(ocid=ocid)

    if collection_id:
        processing_steps = processing_steps.filter(collection_id=collection_id)

    processing_steps = processing_steps.filter(name=step_type)

    if processing_steps.exists():
        processing_steps.delete()
    else:
        logger.warning(
            "No such processing step found: step_type=%s collection_id=%s collection_file_id=%s ocid=%s",
            step_type,
            collection_id,
            collection_file_id,
            ocid,
        )
