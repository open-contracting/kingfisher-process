import logging

from process.forms import (CollectionFileForm, CollectionForm, CollectionNote,
                           CollectionNoteForm)
from process.models import Collection, ProcessingStep

# Get an instance of a logger
logger = logging.getLogger("processor.loader")


def create_collection_file(collection, file_path, steps):
    """
    Creates file for a collection and steps for this file.

    :param Collection collection: collection
    :param str file_path path to file data:
    :param array steps steps to perform:

    :returns: created collection file
    :rtype: CollectionFile

    :raises ValueError: if there is a validation error
    """
    form = CollectionFileForm(dict(collection=collection, filename=file_path))

    if form.is_valid():
        collection_file = form.save()
        logger.debug("Create collection file {}".format(collection_file))
        for step in steps:
            processing_step = ProcessingStep()
            processing_step.collection = collection
            processing_step.collection_file = collection_file
            processing_step.name = step
            processing_step.save()
            logger.debug("Created processing step {}".format(processing_step))

        return collection_file
    else:
        raise ValueError(form.error_messages)


def create_collections(
    source_id, data_version, note=None, upgrade=False, compile=False, check=False, sample=False, force=False
):
    """
    Creates master collection, note, upgraded collection, compiled collection etc. based on provided data

    :param str source_id: collection source
    :param str data_version: data version in ISO format
    :param str note: text description
    :param boolean upgrade: whether to plan collection upgrade
    :param boolean compile: whether to plan collection compile
    :param boolean sample: is this sample only

    :returns: created master collection, upgraded collection, compiled_collection
    :rtype: Collection, Collection, Collection

    :raises ValueError: if there is a validation error
    :raises ValueError: if there is a validation error
    :raises IntegrityError: if such colleciton already exists
    """
    data = {"source_id": source_id, "data_version": data_version, "sample": sample, "force": force}

    collection_steps = []
    if check:
        collection_steps.append("check")

    if upgrade:
        collection_steps.append("upgrade")
    elif compile:
        collection_steps.append("compile")

    # create master collection
    collection = _create_collection(data, collection_steps, note, None, None)

    # handling potential upgrade
    upgraded_collection = None
    if upgrade and compile:
        # master -> upgrade -> compile
        upgraded_collection = _create_collection(
            data, ["compile"], note, collection, Collection.Transforms.UPGRADE_10_11
        )
    if upgrade and not compile:
        # master -> upgrade
        upgraded_collection = _create_collection(data, [], note, collection, Collection.Transforms.UPGRADE_10_11)

    # handling compiled collection
    compiled_collection = None
    if compile and upgraded_collection:
        # master -> upgrade -> compile
        compiled_collection = _create_collection(
            data, [], note, upgraded_collection, Collection.Transforms.COMPILE_RELEASES
        )

    if compile and not upgraded_collection:
        # master -> compile
        compiled_collection = _create_collection(data, [], note, collection, Collection.Transforms.COMPILE_RELEASES)

    return collection, upgraded_collection, compiled_collection


def _create_collection(data, steps, note, parent, transform_type):
    collection_data = data.copy()
    collection_data["steps"] = steps
    collection_data["transform_type"] = transform_type
    collection_data["parent"] = parent

    collection_form = CollectionForm(collection_data)
    if collection_form.is_valid():
        collection = collection_form.save()
        if note:
            _save_note(collection, note)
        return collection

    else:
        raise ValueError(collection_form.error_messages)


def _save_note(collection, note):
    """
    Creates note for a given collection
    """
    form = CollectionNoteForm(dict(collection=collection, note=note, code=CollectionNote.Codes.INFO))
    if form.is_valid():
        return form.save()
    else:
        raise ValueError(form.error_messages)
