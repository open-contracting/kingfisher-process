import argparse
import os
import logging

from django.core.management.base import BaseCommand, CommandError

class BaseWorker(BaseCommand):
    
    loggerInstance = None

    def __init__(self, name):
        self.loggerInstance = logging.getLogger("worker.{}".format(name))

    def file_or_directory(self, string):
        """Checks whether the path is existing file or directory. Raises an exception if not"""
        if not os.path.exists(string):
            raise argparse.ArgumentTypeError(t('No such file or directory %(path)r') % {'path': string})
        return string

    def logger(self):
        """Returns initialised logger instance"""
        return self.loggerInstance

    def handle(self, *args, **options):
        self.logger().debug("Worker started")
        print("aaa")
    
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

