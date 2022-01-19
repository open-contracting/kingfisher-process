import logging

from process.models import Collection, CollectionFile, ProcessingStep

logger = logging.getLogger(__name__)


def completable(collection_id):
    """
    Checks whether the collection can be marked as completed.

    :param int collection_id: collection id - to be checked

    :returns: true if the collection processing was completed
    :rtype: bool
    """

    try:
        collection = Collection.objects.get(pk=collection_id)
    except Collection.DoesNotExist:
        logger.warning("Collection %s not completable (not found)", collection_id)
        return False

    if collection.completed_at:
        logger.warning("Collection %s not completable (already completed)", collection)
        return False

    # compile-releases collections don't set the `store_end_at` field (?) - instead check the root collection.
    if collection.store_end_at is None and (
        collection.transform_type != Collection.Transforms.COMPILE_RELEASES
        or collection.get_root_parent().store_end_at is None
    ):
        logger.debug("Collection %s not completable (load not finished)", collection)
        return False

    # special case when the collection should be compiled and
    # waits for compilation to be planned
    # in such case, no processing steps will be available yet
    if collection.transform_type == Collection.Transforms.COMPILE_RELEASES and not collection.compilation_started:
        logger.debug("Collection %s not completable (compile not started)", collection)
        return False

    has_steps_remaining = ProcessingStep.objects.filter(collection=collection).exists()
    if has_steps_remaining:
        logger.debug("Collection %s not completable (steps remaining)", collection)
        return False

    real_files_count = CollectionFile.objects.filter(collection=collection).count()
    if collection.expected_files_count and collection.expected_files_count > real_files_count:
        logger.debug(
            "Collection %s not completable. There are (probably) some unprocessed messages in the queue with the new "
            "items - expected files count %s, real files count %s",
            collection,
            collection.expected_files_count,
            real_files_count,
        )
        return False

    return True
