from django.core.management import call_command
from django.test import TransactionTestCase

from process.models import (
    CollectionFile,
    CompiledRelease,
    Data,
    PackageData,
    Release,
)
from tests.fixtures import collection


class DeleteOrphanTests(TransactionTestCase):
    def test_deletes_orphans_only(self):
        source = collection()
        source.save()
        collection_file = CollectionFile(collection=source, filename="x.json")
        collection_file.save()

        referenced_data = Data.objects.create(hash_md5="a", data={"ocid": "ocds-1"})
        referenced_package = PackageData.objects.create(hash_md5="b", data={"version": "1.1"})
        compiled_data = Data.objects.create(hash_md5="c", data={"ocid": "ocds-1", "tag": ["compiled"]})

        # Referenced by a release, a compiled release and a release's package: not orphaned.
        Release.objects.create(
            collection=source,
            collection_file=collection_file,
            ocid="ocds-1",
            data=referenced_data,
            package_data=referenced_package,
        )
        CompiledRelease.objects.create(
            collection=source,
            collection_file=collection_file,
            ocid="ocds-1",
            data=compiled_data,
        )

        # Referenced by nothing: orphaned.
        Data.objects.create(hash_md5="d", data={"ocid": "ocds-2"})
        PackageData.objects.create(hash_md5="e", data={"version": "1.0"})

        call_command("deleteorphan", "--force")

        self.assertEqual(set(Data.objects.values_list("hash_md5", flat=True)), {"a", "c"})
        self.assertEqual(set(PackageData.objects.values_list("hash_md5", flat=True)), {"b"})
