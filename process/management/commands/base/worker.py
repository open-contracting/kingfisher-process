import argparse
import functools
import logging
import os
import threading

import pika
from django.conf import settings
from django.core.management.base import BaseCommand
from django.db import connections
from django.utils.translation import gettext as t

from process.models import Collection, CollectionFile, CollectionNote, ProcessingStep
from process.util import get_rabbit_channel


class BaseWorker(BaseCommand):
    logger_instance = None
    rabbit_channel = None
    rabbit_connection = None
    rabbit_consume_routing_keys = []
    rabbit_publish_routing_key = None
    rabbit_consume_queue = None

    prefetch_count = 1

    def __init__(self, name, *args, **kwargs):
        self.logger_instance = logging.getLogger("process.management.commands.{}".format(name))
        if settings.RABBIT_URL:
            self._initMessaging()
        super(BaseWorker, self).__init__(*args, **kwargs)

    def handle(self, *args, **options):
        self._logger().debug("Worker started")
        self._consume(self.process)

    def _initMessaging(self):
        """Connects to RabbitMQ and prepares all the necessities - exchange, proper names for queues etc."""
        self._debug("Connecting to RabbitMQ...")

        self.rabbit_consume_queue = f"{settings.RABBIT_EXCHANGE_NAME}_{self.worker_name}"

        if hasattr(self, "consume_keys") and isinstance(self.consume_keys, list) and self.consume_keys:
            for consumeKey in self.consume_keys:
                self.rabbit_consume_routing_keys.append(f"{settings.RABBIT_EXCHANGE_NAME}_{consumeKey}")
        else:
            self._debug("No or improper defined consume keys, starting without listening to messages.")

        self.rabbit_publish_routing_key = f"{settings.RABBIT_EXCHANGE_NAME}_{self.worker_name}"

        self.rabbit_channel, self.rabbit_connection = get_rabbit_channel(settings.RABBIT_EXCHANGE_NAME)

        self._info("RabbitMQ connection established")

    def _consume(self, target_callback):
        """Define which messages to consume and queue for this worker"""
        # declare queue to store unprocessed messages
        self.rabbit_channel.queue_declare(queue=self.rabbit_consume_queue, durable=True)

        # bind consume keys to the queue
        for consumeKey in self.rabbit_consume_routing_keys:
            self.rabbit_channel.queue_bind(
                exchange=settings.RABBIT_EXCHANGE_NAME, queue=self.rabbit_consume_queue, routing_key=consumeKey
            )

            self._debug(
                "Consuming messages from exchange %s with routing key %s", settings.RABBIT_EXCHANGE_NAME, consumeKey
            )

            self.rabbit_channel.basic_qos(prefetch_count=self.prefetch_count)

            def on_message(channel, method_frame, header_frame, body, args):
                (connection, target_callback) = args
                delivery_tag = method_frame.delivery_tag
                t = threading.Thread(target=target_callback, args=(connection, channel, delivery_tag, body))
                t.start()

            on_message_callback = functools.partial(on_message, args=(self.rabbit_connection, target_callback))
            self.rabbit_channel.basic_consume(queue=self.rabbit_consume_queue, on_message_callback=on_message_callback)

        self.rabbit_channel.start_consuming()

    def _ack(self, connection, channel, delivery_tag):
        self._debug("ACK message with delivery tag %s", delivery_tag)
        cb = functools.partial(self._ack_message, channel, delivery_tag)
        connection.add_callback_threadsafe(cb)

    def _ack_message(self, channel, delivery_tag):
        channel.basic_ack(delivery_tag)

    def _nack(self, connection, channel, delivery_tag):
        self._debug("NACK message from channel %s with delivery tag %s", channel, delivery_tag)
        cb = functools.partial(self._nack_message, channel, delivery_tag)
        connection.add_callback_threadsafe(cb)

    def _nack_message(self, channel, delivery_tag):
        channel.basic_nack(delivery_tag)

    def _publish(self, message, routing_key=None):
        """Publish message with work for a next part of process"""
        if routing_key:
            publish_routing_key = f"{settings.RABBIT_EXCHANGE_NAME}_{routing_key}"
        else:
            publish_routing_key = self.rabbit_publish_routing_key

        self.rabbit_channel.basic_publish(
            exchange=settings.RABBIT_EXCHANGE_NAME,
            routing_key=publish_routing_key,
            body=message,
            properties=pika.BasicProperties(delivery_mode=2),
        )

        self._debug(
            "Published message to exchange %s with routing key %s. Message: %s",
            settings.RABBIT_EXCHANGE_NAME,
            self.rabbit_publish_routing_key,
            message,
        )

    def _publish_async(self, connection, channel, message, routing_key=None):
        """Publish message with work for a next part of process"""
        cb = functools.partial(self._publish_async_callback, channel, message, routing_key)
        connection.add_callback_threadsafe(cb)

    def _publish_async_callback(self, channel, message, routing_key):
        if routing_key:
            publish_routing_key = f"{settings.RABBIT_EXCHANGE_NAME}_{routing_key}"
        else:
            publish_routing_key = self.rabbit_publish_routing_key

        channel.basic_publish(
            exchange=settings.RABBIT_EXCHANGE_NAME,
            routing_key=publish_routing_key,
            body=message,
            properties=pika.BasicProperties(delivery_mode=2),
        )

        self._debug(
            "Published message to exchange %s with routing key %s. Message: %s",
            settings.RABBIT_EXCHANGE_NAME,
            self.rabbit_publish_routing_key,
            message,
        )

    def _clean_thread_resources(self):
        """
        Cleans thread resources, which are not cleaned by default and automatically
        i.e. django db connections.
        """
        for conn in connections.all():
            conn.close()

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

        processing_steps = processing_steps.filter(name=step_type)

        if processing_steps.exists():
            processing_steps.delete()
        else:
            self._warning(
                "No such processing step found: step_type=%s collection_id=%s collection_file_id=%s ocid=%s",
                step_type,
                collection_id,
                collection_file_id,
                ocid,
            )

    def _file_or_directory(self, string):
        """Checks whether the path is existing file or directory. Raises an exception if not"""
        if not os.path.exists(string):
            raise argparse.ArgumentTypeError(t("No such file or directory %(path)r") % {"path": string})
        return string

    def _logger(self):
        """Returns initialised logger instance"""
        return self.logger_instance

    def _debug(self, message, *args, **kwargs):
        """Shortcut function to logging facility"""
        self._logger().debug(message, *args, **kwargs)

    def _info(self, message, *args, **kwargs):
        """Shortcut function to logging facility"""
        self._logger().info(message, *args, **kwargs)

    def _warning(self, message, *args, **kwargs):
        """Shortcut function to logging facility"""
        self._logger().warning(message, *args, **kwargs)

    def _error(self, message, *args, **kwargs):
        """Shortcut function to logging facility"""
        self._logger().error(message, *args, **kwargs)

    def _critical(self, message, *args, **kwargs):
        """Shortcut function to logging facility"""
        self._logger().critical(message, *args, **kwargs)

    def _exception(self, message, *args, **kwargs):
        """Shortcut function to logging facility"""
        self._logger().exception(message, *args, **kwargs)

    def _save_note(self, collection, code, note):
        """Shortcut to save note to collection"""
        collection_note = CollectionNote()
        collection_note.collection = collection
        collection_note.code = code
        collection_note.note = note
        collection_note.save()

    def _get_input(self, message):
        """Gets user input"""
        return input(message)
