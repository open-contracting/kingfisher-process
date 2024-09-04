from urllib.parse import urljoin

import requests
from django.conf import settings


def configured() -> bool:
    """
    Return whether the connection to Scrapyd is configured.
    """
    return bool(settings.SCRAPYD["url"])


def spiders() -> list[str]:
    """
    Return the names of the spiders in the Scrapyd project.
    """
    # https://scrapyd.readthedocs.io/en/stable/api.html#listspiders-json
    url = urljoin(settings.SCRAPYD["url"], "/listspiders.json")
    response = requests.get(url, params={"project": settings.SCRAPYD["project"]}, timeout=10)
    response.raise_for_status()
    return response.json()["spiders"]
