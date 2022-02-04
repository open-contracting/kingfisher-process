import hashlib
import logging
import os
import signal
from contextlib import contextmanager
from textwrap import fill

import pika.exceptions
from django.conf import settings
from django.db import connections
from django.db.utils import IntegrityError
from yapw import clients
from yapw.decorators import decorate
from yapw.methods.blocking import nack

from process.exceptions import AlreadyExists, InvalidFormError
from process.models import CollectionNote, ProcessingStep

logger = logging.getLogger(__name__)

# These must match the output of ocdskit.util.detect_format().
RELEASE_PACKAGE = "release package"
RECORD_PACKAGE = "record package"


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
def get_publisher():
    client = get_client(Publisher)
    try:
        yield client
    finally:
        client.close()


# https://github.com/pika/pika/blob/master/examples/blocking_consume_recover_multiple_hosts.py
def consume(*args, **kwargs):
    while True:
        try:
            client = get_client(Consumer, prefetch_count=20)
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
    Close the database connections opened by the callback, before returning.

    If the callback raises an exception, send the SIGUSR1 signal to the main thread, without acknowledgment. For some
    exceptions, assume that the same message was delivered twice, log an error, and ack the message.
    """

    def errback(exception):
        # These errors should only occur if the RabbitMQ and/or PostgreSQL connection is lost. It's not possible to
        # have a transaction that spans both systems, so it's possible to insert a row then fail to ack a message.
        #
        # That said, we monitor the frequency of these errors via Sentry, to ensure that they are caused by the above
        # and not by an error in logic. Their number should not exceed the prefetch count.
        #
        # InvalidFormError is included, as it may be for a "unique_together" error, which is an integrity error.
        if isinstance(exception, (AlreadyExists, InvalidFormError, IntegrityError)):
            logger.error(f"{exception.__class__.__name__} possibly caused by duplicate message: {exception}")
            nack(state, channel, method.delivery_tag, requeue=False)
        else:
            logger.exception("Unhandled exception when consuming %r, sending SIGUSR1", body)
            os.kill(os.getpid(), signal.SIGUSR1)

    def finalback():
        for conn in connections.all():
            conn.close()

    decorate(decode, callback, state, channel, method, properties, body, errback, finalback)


def create_note(collection, code, note):
    if isinstance(note, list):
        note = " ".join(note)
    CollectionNote(collection=collection, code=code, note=note).save()


def create_step(name, collection_id, **kwargs):
    ProcessingStep(name=name, collection_id=collection_id, **kwargs).save()


@contextmanager
def delete_step(*args, **kwargs):
    try:
        yield
    # See the errback() function in the decorator() function. If a duplicate message is received, we want to ensure
    # that the step was deleted, so that the collection is completable, before re-raising the exception.
    except (AlreadyExists, InvalidFormError, IntegrityError):
        _delete_step(*args, **kwargs)
        raise
    else:
        _delete_step(*args, **kwargs)


def _delete_step(step_type, **kwargs):
    # kwargs can include collection_id, collection_file_id and ocid.
    processing_steps = ProcessingStep.objects.filter(name=step_type, **kwargs)

    if processing_steps.exists():
        processing_steps.delete()
    else:
        logger.warning("No such processing step found: %s: %s", step_type, kwargs)
