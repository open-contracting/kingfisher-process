import argparse
import logging
import os

import pika
from django.conf import settings
from django.core.management.base import BaseCommand
from django.utils.translation import gettext as t

from process.models import Collection, CollectionFile, CollectionNote, ProcessingStep
from process.util import get_env_id, get_rabbit_channel


class BaseWorker(BaseCommand):

    logger_instance = None

    env_id = None

    rabbit_exchange = None

    rabbit_channel = None

    rabbit_consume_routing_keys = []

    rabbit_publish_routing_key = None

    rabbit_consume_queue = None

    def __init__(self, name, *args, **kwargs):
        self.logger_instance = logging.getLogger("worker.{}".format(name))
        self.env_id = get_env_id()
        if settings.RABBITMQ:
            self._initMessaging()
        super(BaseWorker, self).__init__(*args, **kwargs)

    def handle(self, *args, **options):
        self._logger().debug("Worker started")
        self._consume(self.process)

    def _initMessaging(self):
        """Connects to RabbitMQ and prepares all the necessities - exchange, proper names for queues etc."""
        self._debug("Connecting to RabbitMQ...")

        # build queue name
        self.rabbit_consume_queue = "kingfisher_process_{}_{}".format(self.env_id, self.worker_name)

        # build consume keys
        if hasattr(self, "consume_keys") and isinstance(self.consume_keys, list) and self.consume_keys:
            # multiple keys to process
            for consumeKey in self.consume_keys:
                self.rabbit_consume_routing_keys.append("kingfisher_process_{}_{}".format(self.env_id, consumeKey))
        else:
            # undefined consume keys
            self._debug("No or improper defined consume keys, starting without listening to messages.")

        # build publish key
        self.rabbit_publish_routing_key = "kingfisher_process_{}_{}".format(self.env_id, self.worker_name)

        # build exchange name
        self.rabbit_exchange = "kingfisher_process_{}".format(self.env_id)

        self.rabbit_channel = get_rabbit_channel(self.rabbit_exchange)

        self._info("RabbitMQ connection established")

    def _consume(self, callback):
        """Define which messages to consume and queue for this worker"""
        # declare queue to store unprocessed messages
        self.rabbit_channel.queue_declare(queue=self.rabbit_consume_queue, durable=True)

        # bind consume keys to the queue
        for consumeKey in self.rabbit_consume_routing_keys:
            self.rabbit_channel.queue_bind(
                exchange=self.rabbit_exchange, queue=self.rabbit_consume_queue, routing_key=consumeKey
            )

            self._debug(
                "Consuming messages from exchange {} with routing key {}".format(self.rabbit_exchange, consumeKey)
            )

            self.rabbit_channel.basic_qos(prefetch_count=1)
            self.rabbit_channel.basic_consume(queue=self.rabbit_consume_queue, on_message_callback=callback)

        self.rabbit_channel.start_consuming()

    def _publish(self, message, routing_key=None):
        """Publish message with work for a next part of process"""
        if routing_key:
            publish_routing_key = "kingfisher_process_{}_{}".format(self.env_id, routing_key)
        else:
            publish_routing_key = self.rabbit_publish_routing_key

        self.rabbit_channel.basic_publish(
            exchange=self.rabbit_exchange,
            routing_key=publish_routing_key,
            body=message,
            properties=pika.BasicProperties(delivery_mode=2),
        )

        self._debug(
            "Published message to exchange {} with routing key {}. Message: {}".format(
                self.rabbit_exchange, self.rabbit_publish_routing_key, message
            )
        )

    def _createStep(self, step_type=None, collection_id=None, collection_file_id=None, ocid=None):
        """Creates processing step"""
        processing_step = ProcessingStep()
        processing_step.name = step_type
        if collection_file_id:
            processing_step.collection_file = CollectionFile.objects.get(id=collection_file_id)
            processing_step.collection = processing_step.collection_file.collection
        if ocid:
            processing_step.ocid = ocid
            processing_step.collection = Collection.objects.get(id=collection_id)

        processing_step.save()

    def _deleteStep(self, step_type=None, collection_id=None, collection_file_id=None, ocid=None):
        """Delete processing step"""
        processing_steps = ProcessingStep.objects.all()

        if collection_file_id:
            processing_steps = processing_steps.filter(collection_file=collection_file_id)

        if ocid:
            processing_steps = processing_steps.filter(ocid=ocid)

        if collection_id:
            processing_steps = processing_steps.filter(collection__id=collection_id)

        try:
            processing_step = processing_steps.get(name=step_type)
            processing_step.delete()
        except ProcessingStep.DoesNotExist:
            self._error("""No such processing step found
                           step_type:{} collection_id:{} collection_file_id:{} ocid:{}
                        """.format(step_type, collection_id, collection_file_id, ocid))

    def _file_or_directory(self, string):
        """Checks whether the path is existing file or directory. Raises an exception if not"""
        if not os.path.exists(string):
            raise argparse.ArgumentTypeError(t("No such file or directory %(path)r") % {"path": string})
        return string

    def _logger(self):
        """Returns initialised logger instance"""
        return self.logger_instance

    def _debug(self, message):
        """Shortcut function to logging facility"""
        self._logger().debug(message)

    def _info(self, message):
        """Shortcut function to logging facility"""
        self._logger().info(message)

    def _warning(self, message):
        """Shortcut function to logging facility"""
        self._logger().warning(message)

    def _error(self, message):
        """Shortcut function to logging facility"""
        self._logger().error(message)

    def _critical(self, message):
        """Shortcut function to logging facility"""
        self._logger().critical(message)

    def _exception(self, message):
        """Shortcut function to logging facility"""
        self._logger().exception(message)

    def _save_note(self, collection, code, note):
        """Shortcut to save note to collection"""
        collection_note = CollectionNote()
        collection_note.collection = collection
        collection_note.code = code
        collection_note.note = note
        collection_note.save()