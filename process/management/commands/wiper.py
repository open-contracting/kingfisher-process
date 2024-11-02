import logging

from django.conf import settings
from django.core.management.base import BaseCommand
from django.db import connection
from django.utils.translation import gettext as t
from psycopg2 import sql
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


def delete_collection(collection_id):
    tables = [
        ("record", None),  # references collection_file
        ("release", None),  # references collection_file
        ("compiled_release", None),  # references collection_file
        ("processing_step", None),  # references collection_file
        ("collection_file", None),
        ("collection_note", None),
    ]

    if settings.ENABLE_CHECKER:
        tables = [
            ("record_check", "record"),  # references record
            ("release_check", "release"),  # references release
            *tables,
        ]

    # Note: This would skip and pre_delete and post_delete signals (none at time of writing).
    with connection.cursor() as cursor:
        for table, related in tables:
            if related:
                cursor.execute(
                    sql.SQL(
                        "DELETE FROM {table} WHERE {related_id} IN (SELECT id FROM {related} WHERE collection_id = %s)"
                    ).format(
                        table=sql.Identifier(table),
                        related=sql.Identifier(related),
                        related_id=sql.Identifier(f"{related}_id"),
                    ),
                    [collection_id],
                )
            else:
                cursor.execute(
                    sql.SQL("DELETE FROM {table} WHERE collection_id = %s").format(table=sql.Identifier(table)),
                    [collection_id],
                )

    Collection.objects.filter(pk=collection_id).delete()


def callback(client_state, channel, method, properties, input_message):
    collection_id = input_message["collection_id"]

    try:
        collection = Collection.objects.get(pk=collection_id)
    except Collection.DoesNotExist:
        pass
    else:
        if compiled_collection := collection.get_compiled_collection():
            delete_collection(compiled_collection.pk)
        if upgraded_collection := collection.get_upgraded_collection():
            delete_collection(upgraded_collection.pk)

    delete_collection(collection_id)

    ack(client_state, channel, method.delivery_tag)
