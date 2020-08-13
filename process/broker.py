"""
This file encapsulates all interactions with the message broker, RabbitMQ.
"""
from contextlib import contextmanager

import pika
from django.conf import settings

from process.util import json_dumps


@contextmanager
def connect():
    client = RabbitMQClient(settings.AMQP_URL, settings.AMQP_EXCHANGE)
    try:
        yield client
    finally:
        client.close()


class RabbitMQClient:
    def __init__(self, url='amqp://localhost', exchange=''):
        """
        Connects to RabbitMQ, creates a channel, and creates a durable exchange, unless using the default exchange.

        :param str host: the hostname or IP address of the broker
        :param str exchange: the exchange name
        """
        self.exchange = exchange

        self.connection = pika.BlockingConnection(pika.URLParameters(url))
        self.channel = self.connection.channel()

        if exchange:
            self.channel.exchange_declare(durable=True, exchange=self.exchange, exchange_type='direct')

    def declare_queue(self, routing_key):
        """
        Creates a durable queue named after the routing key, and binds it to the exchange, using the routing key as the
        binding key - such that all messages with that routing key will go to this queue.

        :param str routing key: the routing key
        """
        self.channel.queue_declare(durable=True, queue=routing_key)
        self.channel.queue_bind(exchange=self.exchange, queue=routing_key, routing_key=routing_key)

    def publish(self, routing_key, message):
        """
        Ensures the queue exists, and publishes a persistent message with the routing key.

        :param str routing_key: the routing key
        :param message: a JSON-serializable message
        """
        self.declare_queue(routing_key)

        self.channel.basic_publish(exchange=self.exchange, routing_key=routing_key, body=json_dumps(message),
                                   # https://www.rabbitmq.com/publishers.html#message-properties
                                   properties=pika.BasicProperties(delivery_mode=2, content_type='application/json'))

    def close(self):
        """
        Closes the connection to RabbitMQ.
        """
        self.connection.close()
