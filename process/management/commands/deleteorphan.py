from django.core.management.base import BaseCommand
from django.db import connection, transaction
from django.utils.translation import gettext as t
from django.utils.translation import gettext_lazy as _

from process.util import wrap as w


class Command(BaseCommand):
    help = w(t("Delete rows from the data and package_data tables that relate to no collections"))

    def add_arguments(self, parser):
        parser.add_argument("-f", "--force", action="store_true", help=_("delete the rows without prompting"))

    def handle(self, *args, **options):
        if not options["force"]:
            confirm = input("Orphaned rows will be deleted. Do you want to continue? [y/N] ")
            if confirm.lower() != "y":
                return

        self.stderr.write("Working... ", ending="")

        data = (
            """
            SELECT id FROM data WHERE
                NOT EXISTS (SELECT FROM record WHERE data_id = data.id)
                AND NOT EXISTS (SELECT FROM release WHERE data_id = data.id)
                AND NOT EXISTS (SELECT FROM compiled_release WHERE data_id = data.id)
            LIMIT 100000
            """,
            "DELETE FROM data WHERE id IN %s",
        )
        package_data = (
            """
            SELECT id FROM package_data WHERE
                NOT EXISTS (SELECT FROM record WHERE package_data_id = package_data.id)
                AND NOT EXISTS (SELECT FROM release WHERE package_data_id = package_data.id)
            LIMIT 100000
            """,
            "DELETE FROM package_data WHERE id IN %s",
        )

        with connection.cursor() as cursor:
            for select, delete in (data, package_data):
                while True:
                    with transaction.atomic():
                        cursor.execute(select)
                        ids = tuple(row[0] for row in cursor.fetchall())
                        if not ids:
                            break
                        cursor.execute(delete, [ids])
                        self.stderr.write(".", ending="")

        self.stderr.write(self.style.SUCCESS("done"))
