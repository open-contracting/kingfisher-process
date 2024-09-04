import argparse
import copy
import json
import logging
import os

from django.utils.translation import gettext as t

from process.exceptions import InvalidFormError
from process.forms import CollectionFileForm, CollectionForm, CollectionNote, CollectionNoteForm
from process.models import Collection, CollectionFile, ProcessingStep
from process.util import create_note, create_step

logger = logging.getLogger(__name__)


def file_or_directory(path):
    """
    Check whether the path exists. Raise an exception if not.
    """
    if not os.path.exists(path):
        raise argparse.ArgumentTypeError(t("No such file or directory %(path)r") % {"path": path})
    return path


def create_collection_file(collection, filename=None, url=None, errors=None) -> CollectionFile:
    """
    Create file for a collection and steps for this file.

    :param Collection collection: collection
    :param str filename: path to file data
    :param json errors: errors to be stored
    :returns: created collection file
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
    # Identification
    source_id,
    data_version,
    *,
    sample=False,
    # Steps
    upgrade=False,
    compile=False,
    check=False,
    # Other
    scrapyd_job="",
    note="",
    force=False,
) -> tuple[Collection, Collection, Collection]:
    """
    Create the root collection, derived collections and notes.

    :param str source_id: collection source
    :param str data_version: data version in ISO format
    :param boolean sample: is this sample only
    :param boolean upgrade: whether to plan collection upgrade
    :param boolean compile: whether to plan collection compile
    :param boolean check: whether to plan schema-based checks
    :param str scrapyd_job: Scrapyd job ID
    :param str note: text description
    :param boolean force: skip validation of the source_id against the Scrapyd project
    :returns: the root collection, upgraded collection and compiled_collection
    """
    data = {
        "source_id": source_id,
        "data_version": data_version,
        "sample": sample,
        "scrapyd_job": scrapyd_job,
        "force": force,
    }

    steps = []
    if check:
        steps.append("check")
    if upgrade:
        steps.append("upgrade")
    elif compile:
        steps.append("compile")

    collection = _create_collection(data, note, steps=steps)

    upgraded_collection = None
    if upgrade:
        # main -> upgrade -> compile / main -> upgrade
        upgrade_steps = ["compile"] if compile else []
        upgraded_collection = _create_collection(
            data, note, steps=upgrade_steps, parent=collection, transform_type=Collection.Transform.UPGRADE_10_11
        )

    compiled_collection = None
    if compile:
        # main -> upgrade -> compile / main -> compile
        base_collection = upgraded_collection if upgrade else collection
        compiled_collection = _create_collection(
            data, note, parent=base_collection, transform_type=Collection.Transform.COMPILE_RELEASES
        )

    return collection, upgraded_collection, compiled_collection


def _create_collection(data, note, **kwargs):
    collection_data = copy.deepcopy(data)
    collection_data.update(kwargs)
    # If steps is empty, Django attempts to save it as NULL, but the column has a NOT NULL constraint.
    if "steps" in collection_data and not collection_data["steps"]:
        collection_data.pop("steps")

    form = CollectionForm(collection_data)

    if form.is_valid():
        collection = form.save()
        if note:
            _save_note(collection, note)
        return collection

    raise ValueError(form.error_messages)


def _save_note(collection, note):
    form = CollectionNoteForm({"collection": collection, "note": note, "code": CollectionNote.Level.INFO})

    if form.is_valid():
        return form.save()

    raise ValueError(form.error_messages)
