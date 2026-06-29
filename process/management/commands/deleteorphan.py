from django.core.management.base import BaseCommand
from django.db import connection, transaction
from django.utils.translation import gettext as t
from django.utils.translation import gettext_lazy as _

from process.util import wrap as w


class Command(BaseCommand):
    help = w(t("Delete rows in the package_data and data tables that relate to no collections"))

    def add_arguments(self, parser):
        parser.add_argument("-f", "--force", action="store_true", help=_("delete the rows without prompting"))

    def handle(self, *args, **options):
        if not options["force"]:
            confirm = input("Orphaned rows will be deleted. Do you want to continue? [y/N] ")
            if confirm.lower() != "y":
                return

        self.stderr.write("Working... ", ending="")

        with connection.cursor() as cursor:
            # Delete the rows in batches, to bound the size of each transaction.
            for query in (
                """
                WITH batch AS (
                    SELECT id FROM data
                    WHERE id > %s
                        AND NOT EXISTS (SELECT FROM record WHERE data_id = data.id)
                        AND NOT EXISTS (SELECT FROM release WHERE data_id = data.id)
                        AND NOT EXISTS (SELECT FROM compiled_release WHERE data_id = data.id)
                    ORDER BY id LIMIT 100000
                ),
                deleted AS (DELETE FROM data WHERE id IN (SELECT id FROM batch))
                SELECT max(id) FROM batch
                """,
                """
                WITH batch AS (
                    SELECT id FROM package_data
                    WHERE id > %s
                        AND NOT EXISTS (SELECT FROM record WHERE package_data_id = package_data.id)
                        AND NOT EXISTS (SELECT FROM release WHERE package_data_id = package_data.id)
                    ORDER BY id LIMIT 100000
                ),
                deleted AS (DELETE FROM package_data WHERE id IN (SELECT id FROM batch))
                SELECT max(id) FROM batch
                """,
            ):
                last_id = 0
                while True:
                    with transaction.atomic():
                        cursor.execute(query, [last_id])
                        last_id = cursor.fetchone()[0]
                        if last_id is None:
                            break
                        self.stderr.write(".", ending="")

        self.stderr.write(self.style.SUCCESS("done"))
