import logging

from process.models import Collection, CollectionFile, ProcessingStep

# Get an instance of a logger
logger = logging.getLogger(__name__)


def completable(collection_id):
    """
    Checks whether the collection can be marked as completed.

    :param int collection_id: collection id - to be checked

    :returns: true if the collection processing was completed
    :rtype: bool

    :raises ValueError: if there is no collection with such collection_id
    """

    try:
        collection = Collection.objects.get(pk=collection_id)

        if collection.completed_at:
            # already marked as completed in a past
            logger.warning("Collection %s already marked as completed", collection)
            return False

        if collection.store_end_at is not None or (
            collection.store_end_at is None
            and collection.transform_type == Collection.Transforms.COMPILE_RELEASES
            and collection.get_root_parent().store_end_at is not None
        ):

            if (
                collection.transform_type == Collection.Transforms.COMPILE_RELEASES
                and not collection.compilation_started
            ):

                # special case when the collection should be compiled and
                # waits for compilation to be planned
                # in such case, no processing steps will be available yet
                logger.debug("Compilation of collection %s not started yet", collection)

                return False

            has_steps_remaining = ProcessingStep.objects.filter(collection=collection).exists()
            if not has_steps_remaining:
                real_files_count = CollectionFile.objects.filter(collection=collection).count()
                if collection.expected_files_count and collection.expected_files_count > real_files_count:
                    logger.debug(
                        "Collection %s is not completable yet. There are (probably) some"
                        "unprocessed messages in the queue with the new items"
                        " - expected files count %s real files count %s",
                        collection,
                        collection.expected_files_count,
                        real_files_count,
                    )
                    return False

                return True
            else:
                logger.debug("Processing not finished yet for collection %s - >= 1 remaining steps.", collection)
                return False
        else:
            logger.debug("Collection %s not completely stored yet.", collection)
            return False

    except Collection.DoesNotExist:
        logger.info("Collection id %s not found", collection_id)
        return False
