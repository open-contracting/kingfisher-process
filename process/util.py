import hashlib
import logging
import os
from contextlib import contextmanager
from textwrap import fill

import simplejson as json
from django.conf import settings
from django.db import connections, transaction
from django.db.utils import IntegrityError
from yapw.clients import AsyncConsumer, Blocking
from yapw.decorators import decorate
from yapw.methods import add_callback_threadsafe, nack

from process.exceptions import AlreadyExists, InvalidFormError
from process.models import Collection, CollectionFile, CollectionNote, ProcessingStep

logger = logging.getLogger(__name__)

# These must match the output of ocdskit.util.detect_format().
RELEASE_PACKAGE = "release package"
RECORD_PACKAGE = "record package"
COMPILED_RELEASE = "compiled release"
YAPW_KWARGS = {"url": settings.RABBIT_URL, "exchange": settings.RABBIT_EXCHANGE_NAME, "prefetch_count": 20}


def wrap(string):
    """
    Formats a long string as a help message, and returns it.
    """
    return "\n".join(fill(paragraph, width=78, replace_whitespace=False) for paragraph in string.split("\n"))


def walk(paths):
    for path in paths:
        if os.path.isfile(path):
            yield path
        else:
            for root, dirs, files in os.walk(path):
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
        if isinstance(exception, (AlreadyExists, InvalidFormError, IntegrityError)):
            logger.exception("%s maybe caused by duplicate message %r, skipping", exception.__class__.__name__, body)
            nack(state, channel, method.delivery_tag, requeue=False)
        # This error should only occur in the wiper worker due to a duplicate message, as above.
        #
        # It can also occur in the finisher worker if the queue became too long (e.g. the worker was stopped), and the
        # wiper ran before the finisher (e.g. it was run manually by an administrator).
        elif isinstance(exception, Collection.DoesNotExist):
            logger.exception("%s maybe caused by duplicate message %r, skipping", exception.__class__.__name__, body)
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
    hash_md5 = hashlib.md5(
        json.dumps(data, separators=(",", ":"), sort_keys=True, use_decimal=True).encode("utf-8")
    ).hexdigest()

    try:
        obj = model.objects.get(hash_md5=hash_md5)
    except (model.DoesNotExist, model.MultipleObjectsReturned):
        obj = model(data=data, hash_md5=hash_md5)
        try:
            with transaction.atomic():
                obj.save()
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
def delete_step(*args, finish=None, finish_args=(), **kwargs):
    try:
        yield
    # See the errback() function in the decorator() function. If a duplicate message is received, delete the step, so
    # that the collection is completable. Don't delete the step if the error was unexpected.
    except (AlreadyExists, InvalidFormError, IntegrityError):
        _delete_step(*args, **kwargs)
        raise
    else:
        _delete_step(*args, **kwargs)
    finally:
        if finish:
            finish(*finish_args)


def _delete_step(step_type, **kwargs):
    # kwargs can include collection_id, collection_file_id and ocid.
    processing_steps = ProcessingStep.objects.filter(name=step_type, **kwargs)

    if processing_steps.exists():
        processing_steps.delete()
    else:
        logger.warning("No such processing step found: %s: %s", step_type, kwargs)
