from django.core.management.base import CommandError
from django.utils.translation import gettext as t
from django.utils.translation import gettext_lazy as _

from process.cli import CollectionCommand
from process.models import ProcessingStep, Record, Release
from process.util import create_step, get_publisher
from process.util import wrap as w

routing_key = "addchecks"


class Command(CollectionCommand):
    help = w(t("Add processing steps to check data, if unchecked"))

    def handle_collection(self, collection, *args, **options):
        if not collection.store_end_at:
            raise CommandError(
                _("Collection %(id)s is not a closed collection. It was started at %(store_start_at)s.")
                % collection.__dict__
            )

        if collection.parent_id:
            raise CommandError(
                _("Collection %(id)s is not a root collection. Its parent is collection %(parent_id)s.")
                % collection.__dict__
            )

        if "check" not in collection.steps:
            collection.steps.append("check")
            collection.save(update_fields=["steps"])

        for model, related_name in ((Release, "releasecheck"), (Record, "recordcheck")):
            self.stderr.write(
                f"Publishing collection files with missing {model.__name__} checks for collection {collection}"
            )

            # SELECT DISTINCT collection_file_id FROM release
            # LEFT OUTER JOIN release_check ON release.id = release_check.release_id
            # WHERE collection_id = :collection_id AND release_check.id IS NULL
            qs = (
                model.objects.filter(**{"collection": collection, f"{related_name}__isnull": True})
                .values_list("collection_file", flat=True)
                .distinct()
            )

            with get_publisher() as client:
                for collection_file_id in qs.iterator():
                    create_step(ProcessingStep.Name.CHECK, collection.pk, collection_file_id=collection_file_id)
                    message = {"collection_id": collection.pk, "collection_file_id": collection_file_id}
                    client.publish(message, routing_key=routing_key)
