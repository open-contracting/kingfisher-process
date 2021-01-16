import json
import logging
from os.path import isfile

import pika
from django.db import transaction
from django.db.models.functions import Now
from django.http.response import HttpResponseBadRequest, HttpResponseServerError, JsonResponse

from process.models import Collection
from process.processors.loader import create_collection_file as loader_create_collection_file
from process.processors.loader import create_master_collection
from process.util import get_env_id, get_rabbit_channel, json_dumps

logger = logging.getLogger("views.api")


def create_collection(request):
    if request.method == "POST":
        input = json.loads(request.body)
        if "source_id" not in input or "data_version" not in input:
            return HttpResponseBadRequest(
                'Unable to parse input. Please provide {"source_id":"<source_id>", "data_version":"<data_version>"}'
            )

        try:
            collection, upgraded_collection = create_master_collection(
                input["source_id"],
                input["data_version"],
                note=(input.get("note")),
                upgrade=(input.get("upgrade", False)),
                compile=(input.get("compile", False)),
                sample=(input.get("sample", False)),
            )

            result = {}
            result["collection_id"] = collection.id

            if upgraded_collection:
                result["upgraded_collection_id"] = upgraded_collection.id

            return JsonResponse(result)
        except Exception as e:
            response = HttpResponseServerError(e)
            logger.exception("Unable to create collection", e)
            return response
    return HttpResponseBadRequest("Only POST requests accepted")


def create_collection_file(request):
    if request.method == "POST":
        input = json.loads(request.body)

        if "path" not in input or "collection_id" not in input:
            return HttpResponseBadRequest(
                'Unable to parse input. Please provide {"path":"<some_path>", "collection_id":<some_number>}'
            )

        if not isfile(input["path"]):
            return HttpResponseBadRequest("{} is not a file".format(input["path"]))

        try:
            collection = Collection.objects.get(id=input["collection_id"])

            with transaction.atomic():
                collection_file = loader_create_collection_file(collection, input["path"])

                message = {"collection_file_id": collection_file.id}

                if input.get("close", False):
                    collection = Collection.objects.get(id=input["collection_id"])
                    collection.store_end_at = Now()
                    collection.save()

                    upgraded_collection = collection.get_upgraded_collection()
                    if upgraded_collection:
                        upgraded_collection.store_end_at = Now()
                        upgraded_collection.save()

            publish(json_dumps(message))

            return JsonResponse(message)
        except Collection.DoesNotExist:
            error = "Collection file with id {} not found".format(input["collection_id"])
            logger.error(error)
            return HttpResponseServerError(error)
        except Exception as e:
            response = HttpResponseServerError(e)
            logger.exception("Unable to create collection_file", e)
            return response
    return HttpResponseBadRequest("Only POST requests accepted")


def publish(message):
    """Publish message with work for a next part of process"""
    # build exchange name
    rabbit_exchange = "kingfisher_process_{}".format(get_env_id())

    rabbit_channel = get_rabbit_channel(rabbit_exchange)

    # build publish key
    rabbit_publish_routing_key = "kingfisher_process_{}_{}".format(get_env_id(), "api")

    rabbit_channel.basic_publish(
        exchange=rabbit_exchange,
        routing_key=rabbit_publish_routing_key,
        body=message,
        properties=pika.BasicProperties(delivery_mode=2),
    )
