import logging

from django.db.utils import IntegrityError

from process.forms import CollectionForm, CollectionNoteForm
from process.models import Collection

# Get an instance of a logger
logger = logging.getLogger("processor.loader")


def create_master_collection(source_id, data_version, note=None, upgrade=False, compile=False, sample=False):
    """
    Creates master collection, note, upgraded collection etc. based on provided data

    :param int parent_collection_id: collection id - new compiled collection will be created based on this collection

    :returns: id of newly created collection
    :rtype: int

    :raises ValueError: if there is no collection with parent_collection_id or
    :raises ValueError: if the parent collection shouldn't be compiled (no compile in steps)
    :raises AlreadyExists: if the compiled collection was already created
    """
    data = {"source_id": source_id, "data_version": data_version, "sample": sample}

    if upgrade:
        data["steps"] = ["upgrade"]

    collection_form = CollectionForm(data)

    if collection_form.is_valid():
        collection = collection_form.save()

        if note:
            __save_note(collection, note)

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
                    __save_note(upgraded_collection, note)
            else:
                raise ValueError(upgrade_collection_form.error_messages)

        return collection, upgraded_collection
    else:
        raise ValueError(collection_form.error_messages)


def __save_note(collection, note):
    """
    Creates note for a given collection
    """
    form = CollectionNoteForm(dict(collection=collection, note=note))
    if form.is_valid():
        return form.save()
    else:
        raise ValueError(form.error_messages)
