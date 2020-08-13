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
