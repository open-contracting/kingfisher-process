import argparse
import json
import logging
import os

from django.utils.translation import gettext as t

from process.exceptions import InvalidFormError
from process.forms import CollectionFileForm, CollectionForm, CollectionNote, CollectionNoteForm
from process.models import Collection, ProcessingStep
from process.util import create_note, create_step

logger = logging.getLogger(__name__)


def file_or_directory(string):
    """Checks whether the path is existing file or directory. Raises an exception if not"""
    if not os.path.exists(string):
        raise argparse.ArgumentTypeError(t("No such file or directory %(path)r") % {"path": string})
    return string


def create_collection_file(collection, filename=None, url=None, errors=None):
    """
    Creates file for a collection and steps for this file.

    :param Collection collection: collection
    :param str filename: path to file data
    :param json errors: errors to be stored

    :returns: created collection file
    :rtype: CollectionFile

    :raises InvalidFormError: if there is a validation error
    """
    form = CollectionFileForm({"collection": collection, "filename": filename, "url": url})

    if form.is_valid():
        collection_file = form.save()
        if not errors:
            create_step(ProcessingStep.Name.LOAD, collection.pk, collection_file=collection_file)
        else:
            note = f"Couldn't download {collection_file}"  # path is set to url in api_loader
            create_note(collection, CollectionNote.Level.ERROR, note, data=json.loads(errors))

        return collection_file

    raise InvalidFormError(form.error_messages)


def create_collections(
    source_id, data_version, sample=False, note=None, upgrade=False, compile=False, check=False, force=False
):
    """
    Creates main collection, note, upgraded collection, compiled collection etc. based on provided data

    :param str source_id: collection source
    :param str data_version: data version in ISO format
    :param boolean sample: is this sample only
    :param boolean upgrade: whether to plan collection upgrade
    :param boolean compile: whether to plan collection compile
    :param boolean check: whether to plan schema-based checks
    :param str note: text description
    :param boolean force: skip validation of the source_id against the Scrapyd project
    :returns: created main collection, upgraded collection, compiled_collection
    :rtype: Collection, Collection, Collection
    """
    data = {"source_id": source_id, "data_version": data_version, "sample": sample, "force": force}

    steps = []
    if check:
        steps.append("check")
    if upgrade:
        steps.append("upgrade")
    elif compile:
        steps.append("compile")

    collection = _create_collection(data, steps, note, None, None)

    upgraded_collection = None
    if upgrade:
        if compile:  # main -> upgrade -> compile
            upgrade_steps = ["compile"]
        else:  # main -> upgrade
            upgrade_steps = []
        upgraded_collection = _create_collection(
            data, upgrade_steps, note, collection, Collection.Transform.UPGRADE_10_11
        )

    compiled_collection = None
    if compile:
        if upgrade:  # main -> upgrade -> compile
            base_collection = upgraded_collection
        else:  # main -> compile
            base_collection = collection
        compiled_collection = _create_collection(
            data, [], note, base_collection, Collection.Transform.COMPILE_RELEASES
        )

    return collection, upgraded_collection, compiled_collection


def _create_collection(data, steps, note, parent, transform_type):
    collection_data = data.copy()
    collection_data["transform_type"] = transform_type
    collection_data["parent"] = parent
    # If steps is empty, Django attempts to save it as NULL, but the column has a NOT NULL constraint.
    if steps:
        collection_data["steps"] = steps

    form = CollectionForm(collection_data)

    if form.is_valid():
        collection = form.save()
        if note:
            _save_note(collection, note)
        return collection

    raise ValueError(form.error_messages)


def _save_note(collection, note):
    """
    Creates note for a given collection
    """
    form = CollectionNoteForm({"collection": collection, "note": note, "code": CollectionNote.Level.INFO})

    if form.is_valid():
        return form.save()

    raise ValueError(form.error_messages)
