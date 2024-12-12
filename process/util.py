import hashlib
import io
import logging
import os
import warnings
from contextlib import contextmanager
from textwrap import fill

import simplejson as json
from django.conf import settings
from django.db import IntegrityError, connections, transaction
from yapw.clients import AsyncConsumer, Blocking
from yapw.decorators import decorate
from yapw.methods import add_callback_threadsafe, nack

from process.exceptions import AlreadyExists, EmptyFormatError, InvalidFormError
from process.models import Collection, CollectionFile, CollectionNote, ProcessingStep

logger = logging.getLogger(__name__)

YAPW_KWARGS = {"url": settings.RABBIT_URL, "exchange": settings.RABBIT_EXCHANGE_NAME, "prefetch_count": 20}


def wrap(string):
    """Format a long string as a help message, and return it."""
    return "\n".join(fill(paragraph, width=78, replace_whitespace=False) for paragraph in string.split("\n"))


def walk(paths):
    for path in paths:
        if os.path.isfile(path):
            yield path
        else:
            for root, _, files in os.walk(path):
                for name in files:
                    if not name.startswith("."):
                        yield os.path.join(root, name)


@contextmanager
def get_publisher():
    client = Blocking(**YAPW_KWARGS)
    try:
        yield client
    finally:
        client.close()


def consume(*args, **kwargs):
    client = AsyncConsumer(*args, **kwargs, **YAPW_KWARGS)
    client.start()


def decorator(decode, callback, state, channel, method, properties, body):
    """
    Close the database connections opened by the callback, before returning.

    If the callback raises an exception, shut down the client in the main thread, without acknowledgment. For some
    exceptions, assume that the same message was delivered twice, log an error, and nack the message.
    """

    def errback(exception):
        # These errors should only occur if the RabbitMQ and/or PostgreSQL connection is lost. It's not possible to
        # have a transaction that spans both systems, so it's possible to insert a row then fail to ack a message.
        #
        # That said, we monitor the frequency of these errors via Sentry, to ensure that they are caused by the above
        # and not by an error in logic. Their number should not exceed the prefetch count.
        #
        # InvalidFormError is included, as it may be for a "unique_together" error, which is an integrity error.
        #
        # Collection.DoesNotExist should only occur in the wiper worker due to a duplicate message. It can also occur
        # in the finisher worker if the worker was stopped, and the wiper ran before the finisher.
        if isinstance(exception, AlreadyExists | InvalidFormError | IntegrityError | Collection.DoesNotExist):
            logger.exception("%s maybe caused by duplicate message %r, skipping", type(exception).__name__, body)
            nack(state, channel, method.delivery_tag, requeue=False)
        # This error should never occur under normal operations. However, such messages interrupt processing, so they
        # are nack'ed.
        elif isinstance(exception, CollectionFile.DoesNotExist):
            logger.exception("Unprocessable message %r, skipping", body)
            nack(state, channel, method.delivery_tag, requeue=False)
        else:
            logger.exception("Unhandled exception when consuming %r, shutting down gracefully", body)
            add_callback_threadsafe(state.connection, state.interrupt)

    def finalback():
        for conn in connections.all():
            conn.close()

    decorate(decode, callback, state, channel, method, properties, body, errback, finalback)


def get_or_create(model, data):
    """Get or create a Data or PackageData instance."""
    hash_md5 = hashlib.md5(  # noqa: S324 # non-cryptographic
        json.dumps(data, separators=(",", ":"), sort_keys=True, use_decimal=True).encode("utf-8")
    ).hexdigest()

    try:
        # Another transaction is needed here, otherwise a parent transaction catches the integrity error.
        with transaction.atomic():
            obj, created = model.objects.get_or_create(hash_md5=hash_md5, defaults={"data": data})
    # If another transaction in another thread COMMITs the same data after the SELECT, but before the INSERT.
    except IntegrityError:
        obj = model.objects.get(hash_md5=hash_md5)

    return obj


def create_note(collection, code, note, **kwargs):
    if isinstance(note, list):
        note = " ".join(note)
    CollectionNote(collection=collection, code=code, note=note, **kwargs).save()


def create_step(name, collection_id, **kwargs):
    ProcessingStep(name=name, collection_id=collection_id, **kwargs).save()


@contextmanager
def delete_step(*args, **kwargs):
    """Delete the named step and run any finish callback only if successful or if the error is expected."""
    try:
        yield
    # Delete the step so that the collection is completable, only if the error was expected.
    except (
        # See the errback() function in the decorator() function.
        AlreadyExists,
        InvalidFormError,
        IntegrityError,
        # See the try/except block in the callback() function of the file_worker worker.
        EmptyFormatError,
    ):
        _delete_step_and_finish(*args, **kwargs)
        raise
    else:
        _delete_step_and_finish(*args, **kwargs)


def _delete_step_and_finish(name, finish=None, finish_args=(), **kwargs):
    # kwargs can include collection_id, collection_file_id and ocid.
    processing_steps = ProcessingStep.objects.filter(name=name, **kwargs)

    deleted, _ = processing_steps.delete()
    if not deleted:
        logger.warning("No such processing step found: %s: %s", name, kwargs)

    if finish:
        finish(*finish_args)


@contextmanager
def create_warnings_note(collection, category):
    with warnings.catch_warnings(record=True, action="always", category=category) as wlist:
        yield

    note = []
    for w in wlist:
        if issubclass(w.category, category):
            note.append(str(w.message))
        else:
            warnings.warn_explicit(w.message, w.category, w.filename, w.lineno, source=w.source)

    if note:
        create_note(collection, CollectionNote.Level.WARNING, note)


@contextmanager
def create_logger_note(collection, name):
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    handler.setLevel(logging.WARNING)
    logger = logging.getLogger(name)
    logger.addHandler(handler)

    yield

    logger.removeHandler(handler)

    if note := stream.getvalue():
        create_note(collection, CollectionNote.Level.WARNING, note)
