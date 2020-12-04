import argparse
import os
import logging
import pika

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError

class BaseWorker(BaseCommand):
    
    loggerInstance = None

    envId = None

    rabbitExchange = None

    rabbitChannel = None

    rabbitConsumeRoutingKey = None

    rabbitPublishRoutingKey = None

    def __init__(self, name):
        self.loggerInstance = logging.getLogger("worker.{}".format(name))
        self.envId = "{}_{}".format(settings.ENV_NAME, settings.ENV_VERSION)

    def handle(self, *args, **options):
        self.logger().debug("Worker started")
        self.initMessaging()
        self.consume(self.process)

    def initMessaging(self):
        """Connects to RabbitMQ"""
        self.debug("Connecting to RabbitMQ...")

        self.rabbitConsumeRoutingKey = "kingfisher_process_{}_{}".format(self.envId, self.workerName)
        
        self.rabbitPublishRoutingKey = "kingfisher_process_{}_{}".format(self.envId, self.workerName)

        self.rabbitExchange = "kingfisher_process_{}".format(self.envId)

        credentials = pika.PlainCredentials(settings.RABBITMQ["username"],
                                            settings.RABBITMQ["password"])

        connection = pika.BlockingConnection(pika.ConnectionParameters(host=settings.RABBITMQ["host"],
                                                                    port=settings.RABBITMQ["port"],
                                                                    credentials=credentials,
                                                                    blocked_connection_timeout=1800,
                                                                    heartbeat=0))
        self.rabbitChannel = connection.channel()

        self.rabbitChannel.exchange_declare(exchange=self.rabbitExchange,
                            durable='true',
                            exchange_type='direct')
        self.debug("Declared exchange {}".format(self.rabbitExchange))

        self.info("RabbitMQ connection established")


    def consume(self, callback):
        self.rabbitChannel.queue_declare(queue=self.rabbitConsumeRoutingKey, durable=True)

        self.rabbitChannel.queue_bind(exchange=self.rabbitExchange,
                        queue=self.rabbitConsumeRoutingKey,
                        routing_key=self.rabbitConsumeRoutingKey)

        self.rabbitChannel.basic_qos(prefetch_count=1)
        self.rabbitChannel.basic_consume(queue=self.rabbitConsumeRoutingKey, on_message_callback=callback)

        self.debug("Consuming messages from exchange {} with routing key {}".format(
            self.rabbitExchange,
            self.rabbitConsumeRoutingKey))

        self.rabbitChannel.start_consuming()
    

    def publish(self, message):
        self.rabbitChannel.basic_publish(exchange=self.rabbitExchange,
                            routing_key=self.rabbitPublishRoutingKey,
                            body=message,
                            properties=pika.BasicProperties(delivery_mode=2))

        self.debug("Published message to exchange {} with routing key {}. Message: {}".format(
                self.rabbitExchange, self.rabbitPublishRoutingKey, message
            )
        )

    
    def file_or_directory(self, string):
        """Checks whether the path is existing file or directory. Raises an exception if not"""
        if not os.path.exists(string):
            raise argparse.ArgumentTypeError(t('No such file or directory %(path)r') % {'path': string})
        return string

    def logger(self):
        """Returns initialised logger instance"""
        return self.loggerInstance

    def debug(self, message):
        """Shortcut function to logging facility""" 
        self.logger().debug(message)

    def info(self, message):
        """Shortcut function to logging facility""" 
        self.logger().info(message)

    def warning(self, message):
        """Shortcut function to logging facility""" 
        self.logger().warning(message)

    def error(self, message):
        """Shortcut function to logging facility""" 
        self.logger().error(message)

    def critical(self, message):
        """Shortcut function to logging facility""" 
        self.logger().critical(message)

    def exception(self, message):
        """Shortcut function to logging facility""" 
        self.logger().exception(message)
