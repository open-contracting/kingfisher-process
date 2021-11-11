from urllib.parse import urljoin

import requests
from django.conf import settings


def configured():
    """
    :returns: whether the connection to Scrapyd is configured
    :rtype: bool
    """
    return bool(settings.SCRAPYD["url"])


def spiders():
    """
    :returns: the names of the spiders in the Scrapyd project
    :rtype: list
    """
    # https://scrapyd.readthedocs.io/en/stable/api.html#listspiders-json
    url = urljoin(settings.SCRAPYD["url"], "/listspiders.json?project=" + settings.SCRAPYD["project"])
    response = requests.get(url)
    response.raise_for_status()
    return response.json()["spiders"]
