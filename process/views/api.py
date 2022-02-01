import json
import logging
import os
from os.path import isfile

from django.conf import settings
from django.db import transaction
from django.db.models.functions import Now
from django.http.response import HttpResponse, HttpResponseBadRequest, JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_POST

from process.models import Collection, CollectionNote
from process.processors.loader import create_collections
from process.util import get_publisher

logger = logging.getLogger(__name__)


@csrf_exempt
@require_POST
def create_collection(request):
    input_message = json.loads(request.body)
    if "source_id" not in input_message or "data_version" not in input_message:
        return HttpResponseBadRequest(
            'Unable to parse input. Please provide {"source_id": "<source_id>", "data_version": "<data_version>"}'
        )

    if not settings.ENABLE_CHECKER and input_message.get("check"):
        logger.error("Checker is disabled in settings - see ENABLE_CHECKER value.")

    collection, upgraded_collection, compiled_collection = create_collections(
        input_message["source_id"],
        input_message["data_version"],
        note=input_message.get("note"),
        upgrade=input_message.get("upgrade", False),
        compile=input_message.get("compile", False),
        check=input_message.get("check", False),
        sample=input_message.get("sample", False),
    )

    result = {"collection_id": collection.pk}
    if upgraded_collection:
        result["upgraded_collection_id"] = upgraded_collection.pk
    if compiled_collection:
        result["compiled_collection_id"] = compiled_collection.pk

    return JsonResponse(result)


@csrf_exempt
@require_POST
def close_collection(request):
    input_message = json.loads(request.body)

    if "collection_id" not in input_message:
        return HttpResponseBadRequest('Unable to parse input. Please provide {"collection_id": "<collection_id>"}')

    with transaction.atomic():
        collection = Collection.objects.select_for_update().get(pk=input_message["collection_id"])
        if "stats" in input_message and input_message["stats"]:
            # this value is used later on to detect, whether all collection has been processed yet
            collection.expected_files_count = input_message["stats"].get("kingfisher_process_items_sent_rabbit", 0)
        collection.store_end_at = Now()
        collection.save()

        upgraded_collection = collection.get_upgraded_collection()
        if upgraded_collection:
            upgraded_collection.expected_files_count = collection.expected_files_count
            upgraded_collection.store_end_at = Now()
            upgraded_collection.save()

        if "reason" in input_message and input_message["reason"]:
            CollectionNote(
                collection=collection,
                code=CollectionNote.Level.INFO,
                note="Spider close reason: {}".format(input_message["reason"]),
            ).save()

            if upgraded_collection:
                CollectionNote(
                    collection=upgraded_collection,
                    code=CollectionNote.Level.INFO,
                    note="Spider close reason: {}".format(input_message["reason"]),
                ).save()

        if "stats" in input_message and input_message["stats"]:
            CollectionNote(
                collection=collection,
                code=CollectionNote.Level.INFO,
                note="Spider stats",
                data=input_message["stats"],
            ).save()

            if upgraded_collection:
                CollectionNote(
                    collection=upgraded_collection,
                    code=CollectionNote.Level.INFO,
                    note="Spider stats",
                    data=input_message["stats"],
                ).save()

    with get_publisher() as client:
        message = {"collection_id": collection.pk}
        client.publish(message, routing_key="collection_closed")

        if upgraded_collection:
            message = {"collection_id": upgraded_collection.pk}
            client.publish(message, routing_key="collection_closed")

        compiled_collection = collection.get_compiled_collection()
        if compiled_collection:
            message = {"collection_id": compiled_collection.pk}
            client.publish(message, routing_key="collection_closed")

    return HttpResponse(status=204)


@csrf_exempt
@require_POST
def create_collection_file(request):
    input_message = json.loads(request.body)
    if "collection_id" not in input_message or not ("path" in input_message or "errors" in input_message):
        return HttpResponseBadRequest(
            'Unable to parse input. Please provide {"path": "<some_path>", "collection_id": <some_number>}'
        )

    input_path = os.path.join(settings.KINGFISHER_COLLECT_FILES_STORE, input_message["path"])
    if not isfile(input_path):
        return HttpResponseBadRequest("{} is not a file".format(input_path))

    with get_publisher() as client:
        client.publish(input_message, routing_key="api")

    return HttpResponse(status=204)


@csrf_exempt
@require_POST
def wipe_collection(request):
    input_message = json.loads(request.body)
    if "collection_id" not in input_message:
        return HttpResponseBadRequest('Unable to parse input. Please provide {"collection_id":<some_number>}')

    with get_publisher() as client:
        client.publish(input_message, routing_key="wiper")

    return HttpResponse(status=204)
