from django.test import TestCase

from process.models import Collection, CollectionNote
from process.processors.compiler import compile_releases_by_ocdskit


class CompilerTests(TestCase):
    maxDiff = None

    def test_merge_warning(self):
        collection = Collection.objects.create(source_id="test_compiler", data_version="2023-01-01T00:00:00Z")

        releases = [
            {
                "ocid": "ocds-213czf-1",
                "id": "1",
                "date": "2020-01-01T00:00:00Z",
                "parties": [
                    {"id": "ORG-001", "name": "Acme Corp"},
                    {"id": "ORG-001", "name": "Acme Inc."},
                ],
                "awards": [
                    {"id": "1"},
                    {"id": "1"},
                ],
            },
            {
                "ocid": "ocds-213czf-1",
                "id": "2",
                "date": "2020-01-01T00:00:00Z",
                "parties": [
                    {"id": "ORG-002", "name": "Widget Factory"},
                ],
            },
        ]

        result = compile_releases_by_ocdskit(collection, "ocds-213czf-1", releases, set())

        self.assertEqual(
            result,
            {
                "ocid": "ocds-213czf-1",
                "id": "ocds-213czf-1-2020-01-01T00:00:00Z",
                "date": "2020-01-01T00:00:00Z",
                "parties": [
                    {"id": "ORG-001", "name": "Acme Inc."},
                    {"id": "ORG-002", "name": "Widget Factory"},
                ],
                "awards": [{"id": "1"}],
                "tag": ["compiled"],
            },
        )

        self.assertEqual(
            list(CollectionNote.objects.filter(collection=collection).values_list("code", "note", "data")),
            [
                (
                    CollectionNote.Level.WARNING,
                    "Release at index 1 has the same date '2020-01-01T00:00:00Z' as the previous release",
                    {"type": "RepeatedDateValueWarning", "date": "2020-01-01T00:00:00Z", "index": 1},
                ),
                (
                    CollectionNote.Level.WARNING,
                    "Multiple objects have the `id` value 'ORG-001' in the `parties` array\n"
                    "Multiple objects have the `id` value '1' in the `awards` array",
                    {"type": "DuplicateIdValueWarning", "paths": {"parties": 1, "awards": 1}},
                ),
            ],
        )

    def test_merge_error(self):
        collection = Collection.objects.create(source_id="test_compiler", data_version="2023-01-01T00:00:00Z")

        releases = ["invalid release"]

        with self.assertLogs("process.processors.compiler", level="ERROR") as logs:
            result = compile_releases_by_ocdskit(collection, "ocds-213czf-1", releases, set())

        self.assertIsNone(result)

        self.assertEqual(
            list(CollectionNote.objects.filter(collection=collection).values_list("code", "note", "data")),
            [
                (
                    CollectionNote.Level.ERROR,
                    "OCID ocds-213czf-1 can't be compiled",
                    {"type": "NonObjectReleaseError", "message": "Release at index 0 must be an object", "index": 0},
                ),
            ],
        )

        self.assertEqual(len(logs.records), 1)
        self.assertIn("OCID ocds-213czf-1 can't be compiled, skipping", logs.output[0])

    def test_extension_warning(self):
        collection = Collection.objects.create(source_id="test_compiler", data_version="2023-01-01T00:00:00Z")

        releases = [{"ocid": "ocds-213czf-1", "id": "1", "date": "2020-01-01T00:00:00Z"}]

        extensions = {"https://raw.githubusercontent.com/open-contracting/ocds_nonexistent_extension/master/"}

        result = compile_releases_by_ocdskit(collection, "ocds-213czf-1", releases, extensions)

        self.assertEqual(
            result,
            {
                "date": "2020-01-01T00:00:00Z",
                "id": "ocds-213czf-1-2020-01-01T00:00:00Z",
                "ocid": "ocds-213czf-1",
                "tag": ["compiled"],
            },
        )

        self.assertEqual(
            list(CollectionNote.objects.filter(collection=collection).values_list("code", "note", "data")),
            [
                (
                    CollectionNote.Level.WARNING,
                    "https://raw.githubusercontent.com/open-contracting/ocds_nonexistent_extension/master/: "
                    "requests.exceptions.HTTPError: 404 Client Error: Not Found for url: "
                    "https://github.com/open-contracting/ocds_nonexistent_extension/archive/master.zip",
                    {"type": "ExtensionWarning"},
                ),
            ],
        )
