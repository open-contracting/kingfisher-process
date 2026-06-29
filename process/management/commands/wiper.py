import logging

from django.conf import settings
from django.core.management.base import BaseCommand
from django.db import connection
from django.utils.translation import gettext as t
from psycopg import sql
from yapw.methods import ack

from process.models import Collection
from process.util import consume, decorator
from process.util import wrap as w

consume_routing_keys = ["wiper"]
routing_key = "wiper"
logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = w(
        t(
            "Delete collections and their ancestors. Rows in the package_data and data tables are deleted only if "
            "DEDUPLICATE_DATA is disabled; otherwise, use the deleteorphan command to delete them."
        )
    )

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
        # Temp tables are per-session, and concurrent messages run on separate connections.
        if not settings.DEDUPLICATE_DATA:
            cursor.execute("DROP TABLE IF EXISTS wiper_data_ids")
            cursor.execute(
                "CREATE TEMP TABLE wiper_data_ids AS "
                "SELECT data_id AS id FROM release WHERE collection_id = %(id)s "
                "UNION SELECT data_id FROM record WHERE collection_id = %(id)s "
                "UNION SELECT data_id FROM compiled_release WHERE collection_id = %(id)s",
                {"id": collection_id},
            )
            cursor.execute("DROP TABLE IF EXISTS wiper_package_data_ids")
            cursor.execute(
                "CREATE TEMP TABLE wiper_package_data_ids AS "
                "SELECT package_data_id AS id FROM release WHERE collection_id = %(id)s "
                "UNION SELECT package_data_id FROM record WHERE collection_id = %(id)s",
                {"id": collection_id},
            )

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

        if not settings.DEDUPLICATE_DATA:
            cursor.execute("DELETE FROM data WHERE id IN (SELECT id FROM wiper_data_ids)")
            cursor.execute("DELETE FROM package_data WHERE id IN (SELECT id FROM wiper_package_data_ids)")

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
