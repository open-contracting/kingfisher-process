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

    # validate input
    if not isinstance(collection_id, int):
        raise TypeError("collection_id is not an int value")

    try:
        collection = Collection.objects.get(id=collection_id)

        if collection.completed_at:
            # already marked as completed in a past
            logger.warning("Collection {} already marked as completed".format(collection))
            return False

        if (collection.store_end_at is not None or
                (collection.store_end_at is None and
                 collection.transform_type == Collection.Transforms.COMPILE_RELEASES and
                 collection.get_root_parent().store_end_at is not None)):

            if (collection.transform_type == Collection.Transforms.COMPILE_RELEASES
                    and not collection.compilation_started):

                # special case when the collection should be compiled and
                # waits for compilation to be planned
                # in such case, no processing steps will be available yet
                logger.debug(
                    "Compilation of collection {} not started yet".format(collection)
                )

                return False

            processing_step_count = ProcessingStep.objects.filter(collection=collection).count()
            if processing_step_count == 0:
                real_files_count = CollectionFile.objects.filter(collection=collection).count()
                if collection.expected_files_count and collection.expected_files_count > real_files_count:
                    logger.debug("Collection {} is not completable yet. There are (probably) some"
                                 "unprocessed messages in the queue with the new items"
                                 " - expected files count {} real files count {}".format(
                                    collection,
                                    collection.expected_files_count,
                                    real_files_count))
                    return False

                return True
            else:
                logger.debug(
                    "Processing not finished yet for collection {} - remaining {} steps.".format(
                        collection, processing_step_count
                    )
                )
                return False
        else:
            logger.debug("Collection {} not completely stored yet.".format(collection))
            return False

    except Collection.DoesNotExist:
        logger.info("Collection id {} not found".format(collection_id))
        return False