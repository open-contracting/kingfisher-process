import os
from textwrap import fill

import orjson


def json_dumps(data):
    """
    Dumps JSON to a string, and returns it.
    """
    return orjson.dumps(data)


def wrap(string):
    """
    Formats a long string as a help message, and returns it.
    """
    return '\n\n'.join(fill(paragraph, width=78, replace_whitespace=False) for paragraph in string.splitlines())


def walk(paths):
    for path in paths:
        if os.path.isfile(path):
            yield path
        else:
            for root, dirs, files in os.walk(path):
                for name in files:
                    if not name.startswith('.'):
                        yield os.path.join(root, name)
