import glob
import importlib
import inspect
import os
import threading
from _thread import interrupt_main
from contextlib import contextmanager

import ocdskingfisherprocess.cli.commands.base


class TimeoutException(Exception):
    pass


def gather_cli_commands_instances(config=None, database=None):
    commands = {}
    dir_path = os.path.dirname(os.path.realpath(__file__))
    commands_dir = os.path.join(dir_path, 'commands')
    for file in glob.glob(commands_dir + '/*.py'):
        module = importlib.import_module('ocdskingfisherprocess.cli.commands.' + file.split('/')[-1].split('.')[0])
        for item in dir(module):
            value = getattr(module, item)
            if inspect.isclass(value) and issubclass(value, ocdskingfisherprocess.cli.commands.base.CLICommand) \
                    and value is not ocdskingfisherprocess.cli.commands.base.CLICommand:
                commands[getattr(value, 'command')] = value(config=config, database=database)
    return commands


# From https://stackoverflow.com/a/37648512/244258
# See https://github.com/glenfant/stopit#comparing-thread-based-and-signal-based-timeout-control
@contextmanager
def time_limit(seconds, message):
    if seconds is None:
        yield
    elif seconds > 0:
        timer = threading.Timer(seconds, lambda: interrupt_main())  # raises KeyboardInterrupt
        timer.start()
        try:
            yield
        except KeyboardInterrupt:
            raise TimeoutException('Interrupted, or timed out after {:d} seconds: {}'.format(seconds, message))
        finally:
            timer.cancel()
