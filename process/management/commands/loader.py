import argparse
import os
import json
import sys

from process.management.commands.base.worker import BaseWorker
from django.utils.translation import gettext_lazy as _
from django.utils.translation import gettext as t

class Command(BaseWorker):

    workerName = "loader"

    def __init__(self):
        super().__init__(self.workerName)

    def add_arguments(self, parser):
        parser.formatter_class = argparse.RawDescriptionHelpFormatter
        parser.add_argument('PATH', help=_('a file or directory to load'), nargs='+', type=self.file_or_directory)

    def process(self, channel, method, properties, body):
        try:
            # parse input message
            input_message = json.loads(body.decode('utf8'))

            self.debug("Received message {}".format(input_message))
            
            # send message for a next phase
            message = {"dataset_id": dataset_id}
            self.publish(json.dumps(message), get_param("exchange_name") + routing_key)

            channel.basic_ack(delivery_tag=method.delivery_tag)
        except Exception:
            self.exception(
                "Something went wrong when processing {}".format(body))
            sys.exit()