import logging
import os.path
from unittest.mock import patch

from django.core.management import call_command
from django.core.management.base import CommandError
from django.test import TransactionTestCase
from django.test.utils import captured_stderr

from tests.fixtures import collection

logging.getLogger("process.management.commands.load").setLevel(logging.INFO)


def path(filename):
    return os.path.join("tests", "fixtures", filename)


class LoadTests(TransactionTestCase):
    def test_missing_args(self):
        with self.assertRaises(CommandError) as e:
            call_command("load", path("file.json"))

        message = "Please indicate collection source (using --source and --note and, optionally, --time and --sample)"
        self.assertEqual(str(e.exception), message)

    def test_missing_note(self):
        with self.assertRaises(CommandError) as e:
            call_command("load", "--source", "france", path("file.json"))

        self.assertEqual(str(e.exception), "You must add a note (using --note) when loading into a new collection")

    def test_collection_deleted_at(self):
        source = collection(deleted_at="2001-01-01 00:00:00")
        source.save()

        with self.assertRaises(CommandError) as e:
            call_command(
                "load",
                "--source",
                source.source_id,
                path("file.json"),
                "--note",
                "testing",
                "--time",
                "2001-01-01 00:00:00",
            )

        self.assertEqual(str(e.exception), f"A collection {source.pk} matching those arguments is being deleted")

    def test_path_nonexistent(self):
        with self.assertRaises(CommandError) as e:
            call_command("load", "--source", "france", "--note", "x", "nonexistent.json")

        self.assertEqual(str(e.exception), "Error: argument PATH: No such file or directory 'nonexistent.json'")

    def test_path_empty(self):
        with self.assertRaises(CommandError) as e:
            call_command("load", "--source", "france", "--note", "x", path("empty"))

        self.assertEqual(str(e.exception), "No files found")

    def test_time_future(self):
        with self.assertRaises(CommandError) as e:
            call_command("load", "--source", "france", "--note", "x", "--time", "3000-01-01 00:00", path("file.json"))

        message = "'3000-01-01 00:00' is greater than the earliest file modification time: '20"
        self.assertTrue(str(e.exception).startswith(message))

    def test_time_invalid(self):
        for value in ("2000-01-01 00:", "2000-01-01 24:00:00"):
            with self.subTest(value=value):
                with self.assertRaises(CommandError) as e:
                    call_command("load", "--source", "france", "--note", "x", "--time", value, path("file.json"))

                self.assertEqual(
                    str(e.exception),
                    f"data_version '{value}' is not in \"YYYY-MM-DD HH:MM:SS\" format or is an invalid date/time",
                )

    @patch("yapw.methods.blocking.publish")
    def test_source_invalid(self, publish):
        with captured_stderr() as stderr:
            try:
                call_command("load", "--source", "nonexistent", "--note", "x", path("file.json"))
            except Exception as e:
                self.fail(f"Unexpected exception {e}")

            self.assertTrue(
                "The --source argument can't be validated, because a Scrapyd URL is not configured in "
                "settings.py." in stderr.getvalue()
            )

    @patch("process.scrapyd.spiders")
    def test_source_invalid_scrapyd(self, spiders):
        spiders.return_value = ["france"]

        with self.settings(SCRAPYD={"url": "http://", "project": "kingfisher"}):
            with self.assertRaises(CommandError) as e:
                call_command("load", "--source", "nonexistent", "--note", "x", path("file.json"))

            self.assertEqual(
                str(e.exception),
                "source_id: 'nonexistent' is not a spider in the kingfisher project of Scrapyd (can be forced)",
            )

    @patch("process.scrapyd.spiders")
    def test_source_invalid_scrapyd_close(self, spiders):
        spiders.return_value = ["france"]

        with self.settings(SCRAPYD={"url": "http://example.com", "project": "kingfisher"}):
            with self.assertRaises(CommandError) as e:
                call_command("load", "--source", "farnce", "--note", "x", path("file.json"))

            self.assertEqual(
                str(e.exception),
                "source_id: 'farnce' is not a spider in the kingfisher project of Scrapyd. Did you mean: france",
            )

    @patch("process.scrapyd.spiders")
    @patch("yapw.methods.blocking.publish")
    def test_source_invalid_scrapyd_force(self, spiders, publish):
        spiders.return_value = ["france"]

        with self.settings(SCRAPYD={"url": "http://example.com", "project": "kingfisher"}):
            try:
                call_command("load", "--source", "nonexistent", "--note", "x", "--force", path("file.json"))
            except Exception as e:
                self.fail(f"Unexpected exception {e}")

    @patch("process.scrapyd.spiders")
    @patch("yapw.methods.blocking.publish")
    def test_source_local(self, spiders, publish):
        spiders.return_value = ["france"]

        with self.settings(SCRAPYD={"url": "http://example.com", "project": "kingfisher"}):
            try:
                call_command("load", "--source", "france_local", "--note", "x", "--force", path("file.json"))
            except Exception as e:
                self.fail(f"Unexpected exception {e}")

    def test_unique_deleted_at(self):
        source = collection(deleted_at="2001-01-01 00:00:00")
        source.save()

        with self.assertRaises(CommandError) as e:
            call_command(
                "load", "--source", source.source_id, "--time", source.data_version, "--note", "x", path("file.json")
            )

        self.assertEqual(str(e.exception), f"A collection {source.pk} matching those arguments is being deleted")

    def test_unique_store_end_at(self):
        source = collection(store_end_at="2001-01-01 00:00:00")
        source.save()

        with self.assertRaises(CommandError) as e:
            call_command(
                "load", "--source", source.source_id, "--time", source.data_version, "--note", "x", path("file.json")
            )

        self.assertEqual(str(e.exception), f"A closed collection {source.pk} matching those arguments already exists")

    def test_unique(self):
        source = collection()
        source.save()

        with self.assertRaises(CommandError) as e:
            call_command(
                "load", "--source", source.source_id, "--time", source.data_version, "--note", "x", path("file.json")
            )

        self.assertEqual(str(e.exception), f"An open collection {source.pk} matching those arguments already exists.")


# TODO: test files with: bad encoding, nested data
