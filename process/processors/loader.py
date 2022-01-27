import argparse
import logging
import os

from django.utils.translation import gettext as t

from process.exceptions import InvalidFormError
from process.forms import CollectionFileForm, CollectionForm, CollectionNote, CollectionNoteForm
from process.models import Collection, ProcessingStep

logger = logging.getLogger(__name__)


def file_or_directory(string):
    """Checks whether the path is existing file or directory. Raises an exception if not"""
    if not os.path.exists(string):
        raise argparse.ArgumentTypeError(t("No such file or directory %(path)r") % {"path": string})
    return string


def create_collection_file(collection, file_path=None, url=None, errors=None):
    """
    Creates file for a collection and steps for this file.

    :param Collection collection: collection
    :param str file_path path to file data
    :param json errors to be stored

    :returns: created collection file
    :rtype: CollectionFile

    :raises InvalidFormError: if there is a validation error
    """
    form = CollectionFileForm({"collection": collection, "filename": file_path, "url": url})

    if form.is_valid():
        collection_file = form.save()
        logger.debug("Create collection file %s", collection_file)
        if not errors:
            processing_step = ProcessingStep()
            processing_step.collection = collection
            processing_step.collection_file = collection_file
            processing_step.name = ProcessingStep.Types.LOAD
            processing_step.save()
            logger.debug("Created processing step %s", processing_step)
        else:
            collection_note = CollectionNote()
            collection_note.collection = collection
            collection_note.code = (CollectionNote.Codes.ERROR,)
            collection_note.note = "Errors when downloading collection_file_id {} \n{}".format(collection_file, errors)
            collection_note.save()

        return collection_file

    raise InvalidFormError(form.error_messages)


def create_collections(
    source_id, data_version, note=None, upgrade=False, compile=False, check=False, sample=False, force=False
):
    """
    Creates main collection, note, upgraded collection, compiled collection etc. based on provided data

    :param str source_id: collection source
    :param str data_version: data version in ISO format
    :param str note: text description
    :param boolean upgrade: whether to plan collection upgrade
    :param boolean compile: whether to plan collection compile
    :param boolean sample: is this sample only

    :returns: created main collection, upgraded collection, compiled_collection
    :rtype: Collection, Collection, Collection
    """
    data = {"source_id": source_id, "data_version": data_version, "sample": sample, "force": force}

    collection_steps = []
    if check:
        collection_steps.append("check")

    if upgrade:
        collection_steps.append("upgrade")
    elif compile:
        collection_steps.append("compile")

    # create main collection
    collection = _create_collection(data, collection_steps, note, None, None)

    # handling potential upgrade
    upgraded_collection = None
    if upgrade and compile:
        # main -> upgrade -> compile
        upgraded_collection = _create_collection(
            data, ["compile"], note, collection, Collection.Transforms.UPGRADE_10_11
        )
    if upgrade and not compile:
        # main -> upgrade
        upgraded_collection = _create_collection(data, [], note, collection, Collection.Transforms.UPGRADE_10_11)

    # handling compiled collection
    compiled_collection = None
    if compile and upgrade:
        # main -> upgrade -> compile
        compiled_collection = _create_collection(
            data, [], note, upgraded_collection, Collection.Transforms.COMPILE_RELEASES
        )

    if compile and not upgraded_collection:
        # main -> compile
        compiled_collection = _create_collection(data, [], note, collection, Collection.Transforms.COMPILE_RELEASES)

    return collection, upgraded_collection, compiled_collection


def _create_collection(data, steps, note, parent, transform_type):
    collection_data = data.copy()
    collection_data["steps"] = steps
    collection_data["transform_type"] = transform_type
    collection_data["parent"] = parent

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
    form = CollectionNoteForm({"collection": collection, "note": note, "code": CollectionNote.Codes.INFO})

    if form.is_valid():
        return form.save()

    raise ValueError(form.error_messages)
