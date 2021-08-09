import json
import logging
from os.path import isfile

import pika
from django.db import transaction
from django.db.models.functions import Now
from django.http.response import HttpResponse, HttpResponseBadRequest, HttpResponseServerError, JsonResponse
from django.views.decorators.csrf import csrf_exempt

from process.models import Collection, CollectionNote
from process.processors.loader import create_collections
from process.util import get_env_id, get_rabbit_channel, json_dumps

logger = logging.getLogger("views.api")


@csrf_exempt
def create_collection(request):
    if request.method == "POST":
        input = json.loads(request.body)
        if "source_id" not in input or "data_version" not in input:
            return HttpResponseBadRequest(
                'Unable to parse input. Please provide {"source_id":"<source_id>", "data_version":"<data_version>"}'
            )

        try:
            collection, upgraded_collection, compiled_collection = create_collections(
                input["source_id"],
                input["data_version"],
                note=(input.get("note")),
                upgrade=(input.get("upgrade", False)),
                compile=(input.get("compile", False)),
                check=(input.get("check", False)),
                sample=(input.get("sample", False)),
            )

            result = {}
            result["collection_id"] = collection.id

            if upgraded_collection:
                result["upgraded_collection_id"] = upgraded_collection.id

            if compiled_collection:
                result["compiled_collection_id"] = compiled_collection.id

            return JsonResponse(result)
        except Exception as e:
            response = HttpResponseServerError(e)
            logger.exception("Unable to create collection", e)
            return response
    return HttpResponseBadRequest("Only POST requests accepted")


@csrf_exempt
def close_collection(request):
    if request.method == "POST":
        input = json.loads(request.body)

        if "collection_id" not in input:
            return HttpResponseBadRequest(
                'Unable to parse input. Please provide {"collection_id":"<collection_id>"}'
            )

        try:
            with transaction.atomic():
                collection = Collection.objects.select_for_update().get(id=input["collection_id"])
                collection.store_end_at = Now()

                if "stats" in input and input["stats"]:
                    # this value is used later on to detect, whether all collection has been processed yet
                    collection.expected_files_count = input["stats"]["kingfisher_process_items_sent_rabbit"]

                collection.save()
                logger.debug("Collection {} set store_end_at={}".format(collection, collection.store_end_at))
                upgraded_collection = collection.get_upgraded_collection()
                if upgraded_collection:
                    upgraded_collection.expected_files_count = collection.expected_files_count
                    upgraded_collection.store_end_at = Now()
                    upgraded_collection.save()
                    logger.debug("Upgraded collection {} set store_end_at={}".format(upgraded_collection,
                                                                                     upgraded_collection.store_end_at))

                if "reason" in input and input["reason"]:
                    collection_note = CollectionNote()
                    collection_note.collection = collection
                    collection_note.code = CollectionNote.Codes.INFO
                    collection_note.note = "Spider close reason: {}".format(input["reason"])
                    collection_note.save()

                    if upgraded_collection:
                        collection_note = CollectionNote()
                        collection_note.collection = upgraded_collection
                        collection_note.code = CollectionNote.Codes.INFO
                        collection_note.note = "Spider close reason: {}".format(input["reason"])
                        collection_note.save()

                if "stats" in input and input["stats"]:
                    collection_note = CollectionNote()
                    collection_note.collection = collection
                    collection_note.code = CollectionNote.Codes.INFO
                    collection_note.note = "Spider stats"
                    collection_note.data = input["stats"]

                    collection_note.save()

                    if upgraded_collection:
                        collection_note = CollectionNote()
                        collection_note.collection = upgraded_collection
                        collection_note.code = CollectionNote.Codes.INFO
                        collection_note.note = "Spider stats"
                        collection_note.data = input["stats"]
                        collection_note.save()

            message = """{{ "collection_id": {}, "source": "collection_closed" }}"""

            _publish(message.format(collection.id), "collection_closed")
            logger.debug("Published close message for collection {}".format(collection))

            if upgraded_collection:
                _publish(message.format(upgraded_collection.id), "collection_closed")
                logger.debug("Published close message for upgraded collection {}".format(upgraded_collection))

            compiled_collection = collection.get_compiled_collection()
            if compiled_collection:
                _publish(message.format(compiled_collection.id), "collection_closed")
                logger.debug("Published close message for compiled collection {}".format(compiled_collection))

            return HttpResponse("Collection closed")
        except Collection.DoesNotExist:
            error = "Collection with id {} not found".format(input["collection_id"])
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
        input = json.loads(request.body)

        if "collection_id" not in input or not ("path" in input or "errors" in input):
            return HttpResponseBadRequest(
                'Unable to parse input. Please provide {"path":"<some_path>", "collection_id":<some_number>}'
            )

        if not isfile(input["path"]):
            return HttpResponseBadRequest("{} is not a file".format(input["path"]))

        try:
            _publish(json_dumps(input), "api")

            return HttpResponse("Collection file creation planned.")
        except Collection.DoesNotExist:
            error = "Collection file with id {} not found".format(input["collection_id"])
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
        input = json.loads(request.body)

        if "collection_id" not in input:
            return HttpResponseBadRequest(
                'Unable to parse input. Please provide {"collection_id":<some_number>}'
            )

        try:
            _publish(json_dumps(input), "wiper")

            return HttpResponse("Wipe collection {} successfully planned.")
        except Exception as e:
            response = HttpResponseServerError(e)
            logger.exception("Unable to wipe collection", e)
            return response

    return HttpResponseBadRequest("Only POST requests accepted")


def _publish(message, key):
    """Publish message with work for a next part of process"""
    # build exchange name
    rabbit_exchange = "kingfisher_process_{}".format(get_env_id())

    rabbit_channel, rabbit_connection = get_rabbit_channel(rabbit_exchange)

    # build publish key
    rabbit_publish_routing_key = "kingfisher_process_{}_{}".format(get_env_id(), key)

    rabbit_channel.basic_publish(
        exchange=rabbit_exchange,
        routing_key=rabbit_publish_routing_key,
        body=message,
        properties=pika.BasicProperties(delivery_mode=2),
    )
