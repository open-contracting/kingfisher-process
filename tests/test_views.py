from rest_framework.test import APITestCase

from core import settings

base_url = f"/api/{settings.API_VERSION}/collections/"


class CollectionViewTests(APITestCase):
    fixtures = ["tests/fixtures/complete_db.json"]

    def test_openapi(self):
        response = self.client.get("/openapi")

        self.assertEqual(response.status_code, 200)

    def test_swagger_ui(self):
        response = self.client.get("/swagger-ui/")

        self.assertEqual(response.status_code, 200)

    def test_collection_list_ok(self):
        response = self.client.get(f"{base_url}?format=json")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(len(response.data), 3)

    def test_collection_metadata_404(self):
        response = self.client.get(f"{base_url}900/metadata/?format=json")
        self.assertEqual(response.status_code, 404)

    def test_collection_metadata_not_compiled(self):
        response = self.client.get(f"{base_url}1/metadata/?format=json")
        self.assertEqual(response.status_code, 400)

    def test_collection_metadata_ok(self):
        response = self.client.get(f"{base_url}3/metadata/?format=json")
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

    def test_new_400(self):
        response = self.client.post(f"{base_url}new/?format=json", {})
        self.assertEqual(response.status_code, 400)

    def test_new_ok(self):
        response = self.client.post(
            f"{base_url}new/?format=json", {"source_id": "test", "data_version": "2024-04-18 00:00:00"}
        )
        self.assertEqual(response.status_code, 200)

        self.assertJSONEqual(response.content, {"collection_id": 4})

        response = self.client.post(
            f"{base_url}new/?format=json",
            {"source_id": "test2", "data_version": "2024-04-18 00:00:00", "upgrade": True, "compile": True},
        )
        self.assertEqual(response.status_code, 200)

        self.assertJSONEqual(
            response.content, {"collection_id": 5, "upgraded_collection_id": 6, "compiled_collection_id": 7}
        )

    def test_close_400(self):
        response = self.client.post(f"{base_url}close/?format=json", {})
        self.assertEqual(response.status_code, 400)
