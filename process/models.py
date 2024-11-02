from typing import Self

from django.core.exceptions import ValidationError
from django.core.validators import RegexValidator
from django.db import models
from django.utils.translation import gettext_lazy as _

# DjangoJSONEncoder serializes Decimal values as strings. simplejson serializes Decimal values as numbers.
from simplejson import JSONEncoder

# # We set `db_table` so that the table names are identical to those created by SQLAlchemy in an earlier version.
# We don't use `unique=True` or `db_index=True`, because they create an additional index for the text fields `hash_md5`
# and `ocid`. Instead, we set `Meta.constraints` and `Meta.indexes`.
#
# https://docs.djangoproject.com/en/4.2/ref/databases/#indexes-for-varchar-and-text-columns
#
# We don't use default index names (including for foreign key fields) or `%(class)s` in unique constraint names -
# we are explicit, instead - so that the names are identical to those created by SQLAlchemy in an earlier version.
# Otherwise, Django will create a migration to change the name of the index or constraint.


class Default(dict):
    def __getitem__(self, key):
        value = dict.__getitem__(self, key)
        if not value:
            return "{" + key + "}"
        return value


class Collection(models.Model):
    """
    A collection of data from a source.

    There should be at most one collection of a given source (``source_id``) at a given time (``data_version``) of a
    given scope (``sample`` or not). A unique constraint therefore covers these fields.

    A collection can be a sample of a source. For example, an analyst can load a sample of a bulk download, run manual
    queries to check whether it serves their needs, and then load the full file. To avoid the overhead of deleting the
    sample, we instead make ``sample`` part of the unique constraint, along with ``source_id`` and ``data_version``.
    """

    class Transform(models.TextChoices):
        COMPILE_RELEASES = "compile-releases", _("Compile releases")
        UPGRADE_10_11 = "upgrade-1-0-to-1-1", _("Upgrade from 1.0 to 1.1 ")

    # Identification
    source_id = models.TextField(
        validators=[RegexValidator(r"^([a-z]+_)*[a-z]+$", _("source_id must be letters and underscores only"))],
        help_text=_("If sourced from Scrapy, this should be the name of the spider."),
    )
    data_version = models.DateTimeField(help_text=_("The time at which the files were retrieved (not loaded)."))
    sample = models.BooleanField(default=False)

    # Process Manager pattern
    steps = models.JSONField(blank=True, default=list)
    options = models.JSONField(blank=True, default=dict)
    expected_files_count = models.IntegerField(null=True, blank=True)

    # Internal state
    data_type = models.JSONField(blank=True, default=dict)
    compilation_started = models.BooleanField(default=False)

    # Provenance
    parent = models.ForeignKey(
        "self",
        on_delete=models.CASCADE,
        null=True,
        blank=True,
        db_index=False,
        db_column="transform_from_collection_id",
    )
    transform_type = models.TextField(blank=True, choices=Transform.choices)
    scrapyd_job = models.TextField(blank=True)

    # Calculated fields
    cached_releases_count = models.IntegerField(null=True, blank=True)
    cached_records_count = models.IntegerField(null=True, blank=True)
    cached_compiled_releases_count = models.IntegerField(null=True, blank=True)

    # Lifecycle
    store_start_at = models.DateTimeField(auto_now_add=True)
    store_end_at = models.DateTimeField(null=True, blank=True)
    deleted_at = models.DateTimeField(null=True, blank=True)
    completed_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        db_table = "collection"
        indexes = [
            # ForeignKey with db_index=False.
            models.Index(name="collection_transform_from_collection_id_idx", fields=["parent"]),
        ]
        constraints = [
            models.UniqueConstraint(
                name="unique_collection_identifiers",
                fields=["source_id", "data_version", "sample"],
                condition=models.Q(transform_type=""),
                # Django 5 introduces violation_error_code, which can simplify the comparison in load.py.
                # https://docs.djangoproject.com/en/dev/ref/models/constraints/#id1
                violation_error_message=_("A matching collection already exists."),
            ),
            models.UniqueConstraint(name="unique_upgraded_compiled_collection", fields=["parent", "transform_type"]),
        ]

    def __str__(self):
        return "{source_id}:{data_version} (id: {id})".format_map(
            Default(source_id=self.source_id, data_version=self.data_version, id=self.pk)
        )

    # https://docs.djangoproject.com/en/4.2/ref/forms/validation/#raising-validationerror
    def clean_fields(self, exclude=None):
        super().clean_fields(exclude=exclude)

        if bool(self.parent_id) ^ bool(self.transform_type):
            raise ValidationError(
                _("parent and transform_type must either be both set or both not set"), code="field_unpaired"
            )

        if self.parent:
            if self.parent.deleted_at:
                message = _("Parent collection %(id)s is being deleted")
                raise ValidationError(
                    {"parent": ValidationError(message, params=self.parent.__dict__)}, code="parent_deleted"
                )

            if self.transform_type == self.parent.transform_type:
                message = _("Parent collection %(id)s is itself already a transformation of %(parent_id)s")
                if self.parent.transform_type == Collection.Transform.COMPILE_RELEASES:
                    message = _("Parent collection %(id)s is itself already a compilation of %(parent_id)s")
                elif self.parent.transform_type == Collection.Transform.UPGRADE_10_11:
                    message = _("Parent collection %(id)s is itself already an upgrade of %(parent_id)s")
                raise ValidationError(
                    {"transform_type": ValidationError(message, params=self.parent.__dict__)},
                    code="transform_duplicate_transition",
                )

            if (
                self.transform_type == Collection.Transform.UPGRADE_10_11
                and self.parent.transform_type == Collection.Transform.COMPILE_RELEASES
            ):
                message = _("Parent collection %(id)s is compiled and can't be upgraded")
                raise ValidationError(
                    {"transform_type": ValidationError(message, params=self.parent.__dict__)},
                    code="transform_invalid_transition",
                )

            qs = self.parent.collection_set.filter(transform_type=self.transform_type).exclude(pk=self.pk)
            if qs.exists():
                message = _("Parent collection %(source_id)s is already transformed into %(destination_id)s")
                raise ValidationError(
                    message,
                    params={"source_id": self.parent.pk, "destination_id": qs[0].pk},
                    code="transform_duplicated",
                )

    def get_upgraded_collection(self) -> Self | None:
        """Return the upgraded collection or None."""
        # This is a shortcut to avoid a query. It is based on the logic in clean_fields().
        if self.transform_type:
            return None
        try:
            return Collection.objects.get(transform_type=Collection.Transform.UPGRADE_10_11, parent=self)
        except Collection.DoesNotExist:
            return None

    def get_compiled_collection(self) -> Self | None:
        """Return the compiled collection or None."""
        try:
            return Collection.objects.get(transform_type=Collection.Transform.COMPILE_RELEASES, parent=self)
        except Collection.DoesNotExist:
            return None

    def get_root_parent(self) -> Self:
        """Return the "root" ancestor of the collection."""
        if self.parent is None:
            return self
        return self.parent.get_root_parent()


class CollectionNote(models.Model):
    """A note an analyst made about the collection."""

    class Level(models.TextChoices):
        INFO = "INFO"
        ERROR = "ERROR"
        WARNING = "WARNING"

    collection = models.ForeignKey(Collection, on_delete=models.CASCADE, db_index=False)
    note = models.TextField()
    data = models.JSONField(encoder=JSONEncoder, blank=True, default=dict)
    stored_at = models.DateTimeField(auto_now_add=True)
    code = models.TextField(blank=True, choices=Level.choices)

    class Meta:
        db_table = "collection_note"
        indexes = [
            # ForeignKey with db_index=False.
            models.Index(name="collection_note_collection_id_idx", fields=["collection"]),
        ]

    def __str__(self):
        return "{note} (id: {id})".format_map(Default(note=self.note, id=self.pk))


class CollectionFile(models.Model):
    """A file within the collection."""

    collection = models.ForeignKey(Collection, on_delete=models.CASCADE, db_index=False)

    filename = models.TextField(blank=True)
    url = models.TextField(blank=True)

    class Meta:
        db_table = "collection_file"
        indexes = [
            # ForeignKey with db_index=False.
            models.Index(name="collection_file_collection_id_idx", fields=["collection"]),
        ]
        constraints = [
            models.UniqueConstraint(name="unique_collection_file_identifiers", fields=["collection", "filename"]),
        ]

    def __str__(self):
        return "{filename} (id: {id})".format_map(Default(filename=self.filename, id=self.pk))


class ProcessingStep(models.Model):
    """A step in the lifecycle of collection file."""

    class Name(models.TextChoices):
        LOAD = "LOAD"
        COMPILE = "COMPILE"
        CHECK = "CHECK"

    collection = models.ForeignKey(
        Collection, on_delete=models.CASCADE, db_index=False, related_name="processing_steps"
    )

    collection_file = models.ForeignKey(CollectionFile, on_delete=models.CASCADE, null=True, db_index=True)
    ocid = models.TextField(blank=True)
    name = models.TextField(choices=Name.choices)

    class Meta:
        db_table = "processing_step"
        indexes = [
            models.Index(name="processing_step_collection_id_ocid_idx", fields=["collection", "name", "ocid"]),
        ]

    def __str__(self):
        return "{collection_id}:{name} (id: {id})".format_map(
            Default(name=self.name, collection_id=self.collection_id, id=self.pk)
        )


class Data(models.Model):
    """The contents of a release, record or compiled release."""

    hash_md5 = models.TextField()
    data = models.JSONField(encoder=JSONEncoder)

    class Meta:
        db_table = "data"
        constraints = [
            # Used by process.util.get_or_create().
            models.UniqueConstraint(name="unique_data_hash_md5", fields=["hash_md5"]),
        ]

    def __str__(self):
        return "{hash_md5} (id: {id})".format_map(Default(hash_md5=self.hash_md5, id=self.pk))


class PackageData(models.Model):
    """The contents of a package, excluding the releases or records."""

    hash_md5 = models.TextField()
    data = models.JSONField(encoder=JSONEncoder)

    class Meta:
        db_table = "package_data"
        constraints = [
            # Used by process.util.get_or_create().
            models.UniqueConstraint(name="unique_package_data_hash_md5", fields=["hash_md5"]),
        ]

    def __str__(self):
        return "{hash_md5} (id: {id})".format_map(Default(hash_md5=self.hash_md5, id=self.pk))


class Release(models.Model):
    """A release."""

    collection = models.ForeignKey(Collection, on_delete=models.CASCADE, db_index=False)
    collection_file = models.ForeignKey(CollectionFile, on_delete=models.CASCADE, db_index=False)

    ocid = models.TextField(blank=True)
    release_id = models.TextField(blank=True)
    release_date = models.TextField(blank=True)

    data = models.ForeignKey(Data, on_delete=models.CASCADE, db_index=False)
    package_data = models.ForeignKey(PackageData, on_delete=models.CASCADE, db_index=False)

    class Meta:
        db_table = "release"
        indexes = [
            # Used by process.management.commands.release_compiler.compile_release().
            models.Index(fields=["collection", "ocid"]),
            # ForeignKey with db_index=False.
            models.Index(name="release_collection_id_idx", fields=["collection"]),
            models.Index(name="release_collection_file_id_idx", fields=["collection_file"]),
            models.Index(name="release_data_id_idx", fields=["data"]),
            models.Index(name="release_package_data_id_idx", fields=["package_data"]),
        ]
        # It is possible to add a constraint on collection, ocid, release_id. However, some publications have repeated
        # release IDs. Example: https://github.com/open-contracting/kingfisher-collect/issues/1049

    def __str__(self):
        return "{ocid}:{release_id} (id: {id})".format_map(
            Default(ocid=self.ocid, release_id=self.release_id, id=self.pk)
        )


class Record(models.Model):
    """A record."""

    collection = models.ForeignKey(Collection, on_delete=models.CASCADE, db_index=False)
    collection_file = models.ForeignKey(CollectionFile, on_delete=models.CASCADE, db_index=False)

    ocid = models.TextField(blank=True)

    data = models.ForeignKey(Data, on_delete=models.CASCADE, db_index=False)
    package_data = models.ForeignKey(PackageData, on_delete=models.CASCADE, db_index=False)

    class Meta:
        db_table = "record"
        indexes = [
            # Used by process.management.commands.record_compiler.compile_record().
            models.Index(fields=["collection", "ocid"]),
            # ForeignKey with db_index=False.
            models.Index(name="record_collection_id_idx", fields=["collection"]),
            models.Index(name="record_collection_file_id_idx", fields=["collection_file"]),
            models.Index(name="record_data_id_idx", fields=["data"]),
            models.Index(name="record_package_data_id_idx", fields=["package_data"]),
        ]
        # It is possible to add a constraint on collection, ocid. However, some publications have repeated
        # OCIDs. Example: https://github.com/open-contracting/kingfisher-process/issues/420

    def __str__(self):
        return "{ocid} (id: {id})".format_map(Default(ocid=self.ocid, id=self.pk))


class CompiledRelease(models.Model):
    """A compiled release."""

    collection = models.ForeignKey(Collection, on_delete=models.CASCADE, db_index=False)
    collection_file = models.ForeignKey(CollectionFile, on_delete=models.CASCADE, db_index=False)

    ocid = models.TextField(blank=True)
    release_date = models.TextField(blank=True)

    data = models.ForeignKey(Data, on_delete=models.CASCADE, db_index=False)

    class Meta:
        db_table = "compiled_release"
        indexes = [
            # Used by compile_record() and compile_release().
            models.Index(fields=["collection", "ocid"]),
            # ForeignKey with db_index=False.
            models.Index(name="compiled_release_collection_id_idx", fields=["collection"]),
            models.Index(name="compiled_release_collection_file_id_idx", fields=["collection_file"]),
            models.Index(name="compiled_release_data_id_idx", fields=["data"]),
        ]

    def __str__(self):
        return "{ocid} (id: {id})".format_map(Default(ocid=self.ocid, id=self.pk))


class ReleaseCheck(models.Model):
    """The result of checking a release."""

    release = models.OneToOneField(Release, on_delete=models.CASCADE)
    cove_output = models.JSONField()

    class Meta:
        db_table = "release_check"

    def __str__(self):
        return "{release_id} (id: {id})".format_map(Default(release_id=self.release_id, id=self.pk))


class RecordCheck(models.Model):
    """The result of checking a record."""

    record = models.OneToOneField(Record, on_delete=models.CASCADE)
    cove_output = models.JSONField()

    class Meta:
        db_table = "record_check"

    def __str__(self):
        return "{record_id} (id: {id})".format_map(Default(record_id=self.record_id, id=self.pk))
