import json
import sys

from django.db import transaction
from libcoveocds.api import ocds_json_output

from process.management.commands.base.worker import BaseWorker
from process.models import CollectionFile, Release, ReleaseCheck


class Command(BaseWorker):

    worker_name = "checker"

    consume_keys = ["file_worker"]

    def __init__(self):
        super().__init__(self.worker_name)

    def process(self, channel, method, properties, body):
        try:
            # parse input message
            input_message = json.loads(body.decode('utf8'))

            self.debug("Received message {}".format(input_message))

            with transaction.atomic():
                collection_file = CollectionFile.objects.get(pk=input_message["collection_file_id"])

                releases = Release.objects.filter(collection_file_item__collection_file=collection_file)

                for release in releases:
                    self.debug("Checking release {}".format(release))
                    check_result = ocds_json_output("",
                                                    "",
                                                    schema_version="1.0",
                                                    convert=False,
                                                    cache_schema=True,
                                                    file_type="json",
                                                    json_data=release.data.data)

                    releaseCheck = ReleaseCheck()
                    releaseCheck.cove_output = check_result
                    releaseCheck.release = release
                    releaseCheck.save()

                self.deleteStep(collection_file)

            # send message for a next phase
            self.publish(json.dumps(input_message))

            # confirm message processing
            channel.basic_ack(delivery_tag=method.delivery_tag)
        except Exception:
            self.exception(
                "Something went wrong when processing {}".format(body))
            sys.exit()
