import logging

from process.models import Collection, ProcessingStep

# Get an instance of a logger
logger = logging.getLogger("processor.checker")


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

        if collection.store_end_at is not None:
            processing_step_count = ProcessingStep.objects.filter(collection=collection).count()

            if processing_step_count == 0:
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
