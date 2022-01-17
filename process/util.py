import argparse
import functools
import hashlib
import logging
import os
from textwrap import fill

from django.conf import settings
from django.db import connections
from django.utils.translation import gettext as t
from yapw import clients

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


class Client(clients.Threaded, clients.Durable, clients.Blocking, clients.Base):
    pass


@functools.lru_cache(maxsize=None)
def create_client():
    return Client(url=settings.RABBIT_URL, exchange=settings.RABBIT_EXCHANGE_NAME)


def file_or_directory(string):
    """Checks whether the path is existing file or directory. Raises an exception if not"""
    if not os.path.exists(string):
        raise argparse.ArgumentTypeError(t("No such file or directory %(path)r") % {"path": string})
    return string


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


def clean_thread_resources():
    """
    Cleans thread resources, which are not cleaned by default and automatically
    i.e. django db connections.
    """
    for conn in connections.all():
        conn.close()
