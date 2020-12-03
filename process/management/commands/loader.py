import argparse
import os

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

