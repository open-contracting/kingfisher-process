import json
import logging
import os
from os.path import isfile

import pika
from django.conf import settings
from django.db import transaction
from django.db.models.functions import Now
from django.http.response import HttpResponse, HttpResponseBadRequest, HttpResponseServerError, JsonResponse
from django.views.decorators.csrf import csrf_exempt

from process.models import Collection, CollectionNote
from process.processors.loader import create_collections
from process.util import get_rabbit_channel, json_dumps

logger = logging.getLogger(__name__)


@csrf_exempt
def create_collection(request):
    if request.method == "POST":
        input_message = json.loads(request.body)
        if "source_id" not in input_message or "data_version" not in input_message:
            return HttpResponseBadRequest(
                'Unable to parse input. Please provide {"source_id":"<source_id>", "data_version":"<data_version>"}'
            )

        try:
            if not settings.ENABLE_CHECKER and input_message.get("check"):
                logger.error("Checker is disabled in settings - see ENABLE_CHECKER value.")

            collection, upgraded_collection, compiled_collection = create_collections(
                input_message["source_id"],
                input_message["data_version"],
                note=(input_message.get("note")),
                upgrade=(input_message.get("upgrade", False)),
                compile=(input_message.get("compile", False)),
                check=(input_message.get("check", False)),
                sample=(input_message.get("sample", False)),
            )

            result = {}
            result["collection_id"] = collection.pk

            if upgraded_collection:
                result["upgraded_collection_id"] = upgraded_collection.pk

            if compiled_collection:
                result["compiled_collection_id"] = compiled_collection.pk

            return JsonResponse(result)
        except Exception as e:
            response = HttpResponseServerError(e)
            logger.exception("Unable to create collection", e)
            return response
    return HttpResponseBadRequest("Only POST requests accepted")


@csrf_exempt
def close_collection(request):
    if request.method == "POST":
        input_message = json.loads(request.body)

        if "collection_id" not in input_message:
            return HttpResponseBadRequest('Unable to parse input. Please provide {"collection_id":"<collection_id>"}')

        try:
            with transaction.atomic():
                collection = Collection.objects.select_for_update().get(pk=input_message["collection_id"])
                collection.store_end_at = Now()

                if "stats" in input_message and input_message["stats"]:
                    # this value is used later on to detect, whether all collection has been processed yet
                    collection.expected_files_count = input_message["stats"].get(
                        "kingfisher_process_items_sent_rabbit", 0
                    )

                collection.save()
                logger.debug("Collection %s set store_end_at=%s", collection, collection.store_end_at)
                upgraded_collection = collection.get_upgraded_collection()
                if upgraded_collection:
                    upgraded_collection.expected_files_count = collection.expected_files_count
                    upgraded_collection.store_end_at = Now()
                    upgraded_collection.save()
                    logger.debug(
                        "Upgraded collection %s set store_end_at=%s",
                        upgraded_collection,
                        upgraded_collection.store_end_at,
                    )

                if "reason" in input_message and input_message["reason"]:
                    collection_note = CollectionNote()
                    collection_note.collection = collection
                    collection_note.code = CollectionNote.Codes.INFO
                    collection_note.note = "Spider close reason: {}".format(input_message["reason"])
                    collection_note.save()

                    if upgraded_collection:
                        collection_note = CollectionNote()
                        collection_note.collection = upgraded_collection
                        collection_note.code = CollectionNote.Codes.INFO
                        collection_note.note = "Spider close reason: {}".format(input_message["reason"])
                        collection_note.save()

                if "stats" in input_message and input_message["stats"]:
                    collection_note = CollectionNote()
                    collection_note.collection = collection
                    collection_note.code = CollectionNote.Codes.INFO
                    collection_note.note = "Spider stats"
                    collection_note.data = input_message["stats"]

                    collection_note.save()

                    if upgraded_collection:
                        collection_note = CollectionNote()
                        collection_note.collection = upgraded_collection
                        collection_note.code = CollectionNote.Codes.INFO
                        collection_note.note = "Spider stats"
                        collection_note.data = input_message["stats"]
                        collection_note.save()

            message = '{{ "collection_id": {}, "source": "collection_closed" }}'

            _publish(message.format(collection.pk), "collection_closed")
            logger.debug("Published close message for collection %s", collection)

            if upgraded_collection:
                _publish(message.format(upgraded_collection.pk), "collection_closed")
                logger.debug("Published close message for upgraded collection %s", upgraded_collection)

            compiled_collection = collection.get_compiled_collection()
            if compiled_collection:
                _publish(message.format(compiled_collection.pk), "collection_closed")
                logger.debug("Published close message for compiled collection %s", compiled_collection)

            return HttpResponse("Collection closed")
        except Collection.DoesNotExist:
            error = "Collection with id {} not found".format(input_message["collection_id"])
            logger.error(error)
            return HttpResponseServerError(error)
        except Exception as e:
            response = HttpResponseServerError(e)
            logger.exception("Unable to close collection", e)
            return response
    return HttpResponseBadRequest("Only POST requests accepted")


@csrf_exempt
def create_collection_file(request):
    if request.method == "POST":
        input_message = json.loads(request.body)

        if "collection_id" not in input_message or not ("path" in input_message or "errors" in input_message):
            return HttpResponseBadRequest(
                'Unable to parse input. Please provide {"path":"<some_path>", "collection_id":<some_number>}'
            )

        input_path = os.path.join(settings.KINGFISHER_COLLECT_FILES_STORE, input_message["path"])
        if not isfile(input_path):
            return HttpResponseBadRequest("{} is not a file".format(input_path))

        try:
            _publish(json_dumps(input_message), "api")

            return HttpResponse("Collection file creation planned.")
        except Collection.DoesNotExist:
            error = "Collection file with id {} not found".format(input_message["collection_id"])
            logger.error(error)
            return HttpResponseServerError(error)
        except Exception as e:
            response = HttpResponseServerError(e)
            logger.exception("Unable to create collection_file", e)
            return response
    return HttpResponseBadRequest("Only POST requests accepted")


@csrf_exempt
def wipe_collection(request):
    if request.method == "POST":
        input_message = json.loads(request.body)

        if "collection_id" not in input_message:
            return HttpResponseBadRequest('Unable to parse input. Please provide {"collection_id":<some_number>}')

        try:
            _publish(json_dumps(input_message), "wiper")

            return HttpResponse("Wipe collection {} successfully planned.")
        except Exception as e:
            response = HttpResponseServerError(e)
            logger.exception("Unable to wipe collection", e)
            return response

    return HttpResponseBadRequest("Only POST requests accepted")


def _publish(message, key):
    """Publish message with work for a next part of process"""
    rabbit_channel, rabbit_connection = get_rabbit_channel(settings.RABBIT_EXCHANGE_NAME)

    rabbit_publish_routing_key = f"{settings.RABBIT_EXCHANGE_NAME}_{key}"

    rabbit_channel.basic_publish(
        exchange=settings.RABBIT_EXCHANGE_NAME,
        routing_key=rabbit_publish_routing_key,
        body=message,
        properties=pika.BasicProperties(delivery_mode=2),
    )
