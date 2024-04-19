from rest_framework.test import APITestCase

from process.models import Collection, CollectionNote

base_url = "/api/collections"


class CollectionViewTests(APITestCase):
    fixtures = ["tests/fixtures/complete_db.json"]

    def test_openapi(self):
        response = self.client.get("/api/schema/")

        self.assertEqual(response.status_code, 200)

    def test_swagger_ui(self):
        response = self.client.get("/api/schema/swagger-ui/")

        self.assertEqual(response.status_code, 200)

    def test_redoc(self):
        response = self.client.get("/api/schema/redoc/")

        self.assertEqual(response.status_code, 200)

    def test_collection_list_ok(self):
        response = self.client.get(f"{base_url}/?format=json")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(len(response.data), 3)

    def test_collection_metadata_404(self):
        response = self.client.get(f"{base_url}/900/metadata/?format=json")
        self.assertEqual(response.status_code, 404)

    def test_collection_metadata_not_compiled(self):
        response = self.client.get(f"{base_url}/1/metadata/?format=json")
        self.assertEqual(response.status_code, 400)

    def test_collection_metadata_ok(self):
        response = self.client.get(f"{base_url}/3/metadata/?format=json")
        self.assertEqual(response.status_code, 200)
        self.assertJSONEqual(
            response.content,
            {
                "license": "https://creativecommons.org/licenses/by/4.0/",
                "ocid_prefix": "ocds-px0z7d",
                "publication_policy": "http://base.gov.pt/policy/policy.html",
                "published_from": "2019-11-25T17:20:38.079756Z",
                "published_to": "2019-11-25T17:20:44.9244234Z",
            },
        )

    def test_create_400(self):
        response = self.client.post(f"{base_url}/", {})
        self.assertEqual(response.status_code, 400)

    def test_create_ok(self):
        response = self.client.post(f"{base_url}/", {"source_id": "test", "data_version": "2024-04-18 00:00:00"})
        self.assertEqual(response.status_code, 200)

        self.assertJSONEqual(response.content, {"collection_id": 4})

        response = self.client.post(
            f"{base_url}/",
            {"source_id": "test2", "data_version": "2024-04-18 00:00:00", "upgrade": True, "compile": True},
        )
        self.assertEqual(response.status_code, 200)

        self.assertJSONEqual(
            response.content, {"collection_id": 5, "upgraded_collection_id": 6, "compiled_collection_id": 7}
        )

    def test_close_404(self):
        response = self.client.post(f"{base_url}/100/close/", {})
        self.assertEqual(response.status_code, 404)

    def test_close_ok(self):
        response = self.client.post(f"{base_url}/1/close/", {})
        self.assertEqual(response.status_code, 202)

    def test_close_ok_full(self):
        data = {"stats": {"kingfisher_process_expected_files_count": 1}, "reason": "finished"}

        collection_id = 1

        response = self.client.post(f"{base_url}/{collection_id}/close/", data, format="json")
        self.assertEqual(response.status_code, 202)

        collection = Collection.objects.get(id=collection_id)
        upgraded_collection = collection.get_upgraded_collection()

        for c in [collection, upgraded_collection]:
            self.assertEqual(c.expected_files_count, data["stats"]["kingfisher_process_expected_files_count"])
            self.assertIsNotNone(c.store_end_at)

        notes_reason = CollectionNote.objects.filter(
            collection_id__in=[collection_id, upgraded_collection.id], note=f"Spider close reason: {data['reason']}"
        )

        self.assertEqual(len(notes_reason), 2)

        notes_reason_stats = CollectionNote.objects.filter(
            collection_id__in=[collection_id, upgraded_collection.id], note="Spider stats", data=data["stats"]
        )

        self.assertEqual(len(notes_reason_stats), 2)

    def test_destroy_404(self):
        response = self.client.delete(f"{base_url}/100/")
        self.assertEqual(response.status_code, 404)

    def test_destroy_ok(self):
        response = self.client.delete(f"{base_url}/1/")
        self.assertEqual(response.status_code, 202)

    def test_retrieve_404(self):
        response = self.client.get("/api/tree/2/")
        self.assertEqual(response.status_code, 404)

    def test_retrieve_ok(self):
        response = self.client.get("/api/tree/1/")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(len(response.data), 3)
