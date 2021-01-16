import logging

from django.conf import settings

from process.forms import CollectionFileForm, CollectionForm, CollectionNoteForm
from process.models import Collection, CollectionFileStep

# Get an instance of a logger
logger = logging.getLogger("processor.loader")


def create_collection_file(collection, file_path):
    """
    Creates file for a collection and steps for this file.

    :param Collection collection: collection
    :param str file_path:

    :returns: created collection file
    :rtype: CollectionFile

    :raises ValueError: if there is a validation error
    """
    form = CollectionFileForm(dict(collection=collection, filename=file_path))

    if form.is_valid():
        collection_file = form.save()
        logger.debug("Create collection file {}".format(collection_file))
        for step in settings.DEFAULT_STEPS:
            collection_file_step = CollectionFileStep()
            collection_file_step.collection_file = collection_file
            collection_file_step.name = step
            collection_file_step.save()
            logger.debug("Created collection file step {}".format(collection_file_step))

        return collection_file
    else:
        raise ValueError(form.error_messages)


def create_master_collection(
    source_id, data_version, note=None, upgrade=False, compile=False, sample=False, force=False
):
    """
    Creates master collection, note, upgraded collection etc. based on provided data

    :param str source_id: collection source
    :param str data_version: data version in ISO format
    :param str note: text description
    :param boolean upgrade: whether to plan collection upgrade
    :param boolean compile: whether to plan collection compile
    :param boolean sample: is this sample only

    :returns: created master collection and potentialy upgraded collection
    :rtype: Collection, Collection

    :raises ValueError: if there is a validation error
    :raises ValueError: if there is a validation error
    :raises IntegrityError: if such colleciton already exists
    """
    data = {"source_id": source_id, "data_version": data_version, "sample": sample, "force": force}

    if upgrade:
        data["steps"] = ["upgrade"]

    collection_form = CollectionForm(data)

    if collection_form.is_valid():
        collection = collection_form.save()

        if note:
            _save_note(collection, note)

        upgraded_collection = None
        if upgrade:
            if compile:
                data["steps"] = ["compile"]
            else:
                data["steps"] = None

            data["transform_type"] = Collection.Transforms.UPGRADE_10_11
            data["parent"] = collection

            upgrade_collection_form = CollectionForm(data)
            if upgrade_collection_form.is_valid():
                upgraded_collection = upgrade_collection_form.save()
                if note:
                    _save_note(upgraded_collection, note)
            else:
                raise ValueError(upgrade_collection_form.error_messages)

        return collection, upgraded_collection
    else:
        raise ValueError(collection_form.error_messages)


def _save_note(collection, note):
    """
    Creates note for a given collection
    """
    form = CollectionNoteForm(dict(collection=collection, note=note))
    if form.is_valid():
        return form.save()
    else:
        raise ValueError(form.error_messages)
