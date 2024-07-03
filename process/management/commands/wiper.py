import logging

from django.conf import settings
from django.core.management.base import BaseCommand
from django.db import connection
from django.utils.translation import gettext as t
from yapw.methods import ack

from process.models import Collection
from process.util import consume, decorator
from process.util import wrap as w

consume_routing_keys = ["wiper"]
routing_key = "wiper"
logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = w(t("Delete collections and their ancestors. Rows in the package_data and data tables are not deleted."))

    def handle(self, *args, **options):
        consume(
            on_message_callback=callback, queue=routing_key, routing_keys=consume_routing_keys, decorator=decorator
        )


def callback(client_state, channel, method, properties, input_message):
    collection_id = input_message["collection_id"]

    tables = [
        ("record", None),  # references collection_file_item
        ("release", None),  # references collection_file_item
        ("compiled_release", None),  # references collection_file_item
        ("collection_file_item", "collection_file"),  # references collection_file
        ("processing_step", None),  # references collection_file
        ("collection_file", None),
        ("collection_note", None),
    ]

    if settings.ENABLE_CHECKER:
        tables = [("record_check", "record"), ("release_check", "release")] + tables

    # Note: This would skip and pre_delete and post_delete signals (none at time of writing).
    with connection.cursor() as cursor:
        for table, related in tables:
            if related:
                cursor.execute(
                    f"DELETE FROM {table} WHERE {related}_id IN (SELECT id FROM {related} WHERE collection_id = %s)",
                    [collection_id],
                )
            else:
                cursor.execute(
                    f"DELETE FROM {table} WHERE collection_id = %s",
                    [collection_id],
                )

    Collection.objects.filter(pk=collection_id).delete()

    ack(client_state, channel, method.delivery_tag)
