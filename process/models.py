from django.core.exceptions import ValidationError
from django.db import models, transaction
from django.utils.translation import gettext_lazy as _

# DjangoJSONEncoder serializes Decimal values as strings. simplejson serializes Decimal values as numbers.
from simplejson import JSONEncoder

# # We set `db_table` so that the table names are identical to those created by SQLAlchemy in an earlier version. We
# don't use `unique=True` or `db_index=True`, because they create an additional index for the text fields `hash_md5`
# and `ocid`. Instead, we set `Meta.constraints` and `Meta.indexes`.
#
# https://docs.djangoproject.com/en/3.2/ref/databases/#indexes-for-varchar-and-text-columns
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

    class Meta:
        db_table = "collection"
        indexes = [
            models.Index(name="collection_transform_from_collection_id_idx", fields=["parent"]),
        ]
        constraints = [
            models.UniqueConstraint(
                name="unique_collection_identifiers",
                fields=["source_id", "data_version", "sample"],
                condition=models.Q(transform_type=""),
            ),
            models.UniqueConstraint(name="unique_upgraded_compiled_collection", fields=["parent", "transform_type"]),
        ]

    class Transform(models.TextChoices):
        COMPILE_RELEASES = "compile-releases", _("Compile releases")
        UPGRADE_10_11 = "upgrade-1-0-to-1-1", _("Upgrade from 1.0 to 1.1 ")

    # Identification
    source_id = models.TextField(help_text=_("If sourced from Scrapy, this should be the name of the spider."))
    data_version = models.DateTimeField(help_text=_("The time at which the data was collected (not loaded)."))
    sample = models.BooleanField(default=False)

    # Routing slip
    steps = models.JSONField(blank=True, null=True, default=dict)
    options = models.JSONField(blank=True, default=dict)
    expected_files_count = models.IntegerField(null=True, blank=True)

    # Process management
    data_type = models.JSONField(null=True, blank=True)
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

    # Calculated fields
    cached_releases_count = models.IntegerField(null=True, blank=True)
    cached_records_count = models.IntegerField(null=True, blank=True)
    cached_compiled_releases_count = models.IntegerField(null=True, blank=True)

    # Lifecycle
    store_start_at = models.DateTimeField(auto_now_add=True)
    store_end_at = models.DateTimeField(null=True, blank=True)
    deleted_at = models.DateTimeField(null=True, blank=True)
    completed_at = models.DateTimeField(null=True, blank=True)

    def __str__(self):
        return "{source_id}:{data_version} (id: {id})".format_map(
            Default(source_id=self.source_id, data_version=self.data_version, id=self.pk)
        )

    @transaction.atomic()
    def add_step(self, step):
        """
        Adds a step to the collection's processing pipeline. If the step is a transformation, returns the transformed
        collection. If the collection had not yet been saved, this method will save it.
        """
        self.steps[step] = True
        self.full_clean()
        self.save()

        if step in Collection.Transform.values:
            collection = Collection(
                source_id=self.source_id,
                data_version=self.data_version,
                sample=self.sample,
                expected_files_count=self.expected_files_count,
                parent=self,
                transform_type=step,
            )
            collection.full_clean()
            collection.save()
            return collection

    # https://docs.djangoproject.com/en/3.2/ref/forms/validation/#raising-validationerror
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

            if self.transform_type == Collection.Transform.UPGRADE_10_11:
                if self.parent.transform_type == Collection.Transform.COMPILE_RELEASES:
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

    def get_upgraded_collection(self):
        """
        Returns existing upgraded collection or None.

        :returns: upgraded collection
        :rtype: Collection
        """
        # This is a shortcut to avoid a query. It is based on the logic in clean_fields().
        if self.transform_type:
            return None
        try:
            return Collection.objects.get(transform_type=Collection.Transform.UPGRADE_10_11, parent=self)
        except Collection.DoesNotExist:
            return None

    def get_compiled_collection(self):
        """
        Returns existing compiled collection or None.

        :returns: compiled collection
        :rtype: Collection
        """
        try:
            return Collection.objects.get(transform_type=Collection.Transform.COMPILE_RELEASES, parent=self)
        except Collection.DoesNotExist:
            return None

    def get_root_parent(self):
        """
        Returns "root" parent of collection. Basically traverses the tree to the top.

        :returns: root collection
        :rtype: Collection
        """
        if self.parent is None:
            return self
        else:
            return self.parent.get_root_parent()


class CollectionNote(models.Model):
    """
    A note an analyst made about the collection.
    """

    class Meta:
        db_table = "collection_note"
        indexes = [
            models.Index(name="collection_note_collection_id_idx", fields=["collection"]),
        ]

    collection = models.ForeignKey(Collection, on_delete=models.CASCADE, db_index=False)
    note = models.TextField()
    data = models.JSONField(encoder=JSONEncoder, null=True, blank=True)
    stored_at = models.DateTimeField(auto_now_add=True)

    class Level(models.TextChoices):
        INFO = "INFO"
        ERROR = "ERROR"
        WARNING = "WARNING"

    code = models.TextField(blank=True, choices=Level.choices)

    def __str__(self):
        return "{note} (id: {id})".format_map(Default(note=self.note, id=self.pk))


class CollectionFile(models.Model):
    """
    A file within the collection.
    """

    class Meta:
        db_table = "collection_file"
        indexes = [
            models.Index(name="collection_file_collection_id_idx", fields=["collection"]),
        ]
        constraints = [
            models.UniqueConstraint(name="unique_collection_file_identifiers", fields=["collection", "filename"]),
        ]

    collection = models.ForeignKey(Collection, on_delete=models.CASCADE, db_index=False)

    filename = models.TextField(blank=True)
    url = models.TextField(blank=True)

    warnings = models.JSONField(null=True, blank=True)
    errors = models.JSONField(null=True, blank=True)

    def __str__(self):
        return "{filename} (id: {id})".format_map(Default(filename=self.filename, id=self.pk))


class ProcessingStep(models.Model):
    """
    A step in the lifecycle of collection file.
    """

    class Meta:
        db_table = "processing_step"
        indexes = [
            models.Index(name="processing_step_collection_file_id_idx", fields=["collection_file"]),
            models.Index(name="processing_step_ocid_idx", fields=["ocid"]),
            models.Index(name="processing_step_name_idx", fields=["name"]),
        ]

    class Name(models.TextChoices):
        LOAD = "LOAD"
        UPGRADE = "UPGRADE"
        COMPILE = "COMPILE"
        CHECK = "CHECK"

    collection = models.ForeignKey(
        Collection, null=True, on_delete=models.CASCADE, db_index=True, related_name="processing_steps"
    )

    collection_file = models.ForeignKey(CollectionFile, null=True, on_delete=models.CASCADE, db_index=True)
    ocid = models.TextField(null=True)
    name = models.TextField(choices=Name.choices, db_index=True)


class CollectionFileItem(models.Model):
    """
    An item within a file in the collection.
    """

    class Meta:
        db_table = "collection_file_item"
        indexes = [
            models.Index(name="collection_file_item_collection_file_id_idx", fields=["collection_file"]),
        ]
        constraints = [
            models.UniqueConstraint(
                name="unique_collection_file_item_identifiers", fields=["collection_file", "number"]
            ),
        ]

    collection_file = models.ForeignKey(CollectionFile, on_delete=models.CASCADE, db_index=False)

    number = models.IntegerField(blank=True)

    warnings = models.JSONField(null=True, blank=True)
    errors = models.JSONField(null=True, blank=True)

    def __str__(self):
        return "step no. {number} (id: {id})".format_map(Default(number=self.number, id=self.pk))


class Data(models.Model):
    """
    The contents of a release, record or compiled release.
    """

    class Meta:
        db_table = "data"
        constraints = [
            models.UniqueConstraint(name="unique_data_hash_md5", fields=["hash_md5"]),
        ]

    hash_md5 = models.TextField()
    data = models.JSONField(encoder=JSONEncoder)

    def __str__(self):
        return "{hash_md5} (id: {id})".format_map(Default(hash_md5=self.hash_md5, id=self.pk))


class PackageData(models.Model):
    """
    The contents of a package, excluding the releases or records.
    """

    class Meta:
        db_table = "package_data"
        constraints = [
            models.UniqueConstraint(name="unique_package_data_hash_md5", fields=["hash_md5"]),
        ]

    hash_md5 = models.TextField()
    data = models.JSONField(encoder=JSONEncoder)

    def __str__(self):
        return self.hash_md5


class Release(models.Model):
    """
    A release.
    """

    class Meta:
        db_table = "release"
        indexes = [
            models.Index(name="release_collection_id_idx", fields=["collection"]),
            models.Index(name="release_collection_file_item_id_idx", fields=["collection_file_item"]),
            models.Index(name="release_ocid_idx", fields=["ocid"]),
            models.Index(name="release_data_id_idx", fields=["data"]),
            models.Index(name="release_package_data_id_idx", fields=["package_data"]),
        ]

    collection = models.ForeignKey(Collection, on_delete=models.CASCADE, db_index=False)
    collection_file_item = models.ForeignKey(CollectionFileItem, on_delete=models.CASCADE, db_index=False)

    release_id = models.TextField(blank=True)
    ocid = models.TextField(blank=True)

    data = models.ForeignKey(Data, on_delete=models.CASCADE, db_index=False)
    package_data = models.ForeignKey(PackageData, on_delete=models.CASCADE, db_index=False)

    def __str__(self):
        return "{ocid}:{release_id} (id: {id})".format_map(
            Default(ocid=self.ocid, release_id=self.release_id, id=self.pk)
        )


class Record(models.Model):
    """
    A record.
    """

    class Meta:
        db_table = "record"
        indexes = [
            models.Index(name="record_collection_id_idx", fields=["collection"]),
            models.Index(name="record_collection_file_item_id_idx", fields=["collection_file_item"]),
            models.Index(name="record_ocid_idx", fields=["ocid"]),
            models.Index(name="record_data_id_idx", fields=["data"]),
            models.Index(name="record_package_data_id_idx", fields=["package_data"]),
        ]

    collection = models.ForeignKey(Collection, on_delete=models.CASCADE, db_index=False)
    collection_file_item = models.ForeignKey(CollectionFileItem, on_delete=models.CASCADE, db_index=False)

    ocid = models.TextField(blank=True)

    data = models.ForeignKey(Data, on_delete=models.CASCADE, db_index=False)
    package_data = models.ForeignKey(PackageData, on_delete=models.CASCADE, db_index=False)

    def __str__(self):
        return "{ocid} (id: {id})".format_map(Default(ocid=self.ocid, id=self.pk))


class CompiledRelease(models.Model):
    """
    A compiled release.
    """

    class Meta:
        db_table = "compiled_release"
        indexes = [
            models.Index(name="compiled_release_collection_id_idx", fields=["collection"]),
            models.Index(name="compiled_release_collection_file_item_id_idx", fields=["collection_file_item"]),
            models.Index(name="compiled_release_ocid_idx", fields=["ocid"]),
            models.Index(name="compiled_release_data_id_idx", fields=["data"]),
        ]

    collection = models.ForeignKey(Collection, on_delete=models.CASCADE, db_index=False)
    collection_file_item = models.ForeignKey(CollectionFileItem, on_delete=models.CASCADE, db_index=False)

    ocid = models.TextField(blank=True)

    data = models.ForeignKey(Data, on_delete=models.CASCADE, db_index=False)

    def __str__(self):
        return "{ocid} (id: {id})".format_map(Default(ocid=self.ocid, id=self.pk))


class ReleaseCheck(models.Model):
    """
    The result of checking a release.
    """

    class Meta:
        db_table = "release_check"
        indexes = [
            models.Index(name="release_check_release_id_idx", fields=["release"]),
        ]
        constraints = [
            models.UniqueConstraint(
                name="unique_release_check_release_id_and_more", fields=["release", "override_schema_version"]
            ),
        ]

    release = models.ForeignKey(Release, on_delete=models.CASCADE, db_index=False)
    override_schema_version = models.TextField(blank=True)
    cove_output = models.JSONField()


class RecordCheck(models.Model):
    """
    The result of checking a record.
    """

    class Meta:
        db_table = "record_check"
        indexes = [
            models.Index(name="record_check_record_id_idx", fields=["record"]),
        ]
        constraints = [
            models.UniqueConstraint(
                name="unique_record_check_record_id_and_more", fields=["record", "override_schema_version"]
            ),
        ]

    record = models.ForeignKey(Record, on_delete=models.CASCADE, db_index=False)
    override_schema_version = models.TextField(blank=True)
    cove_output = models.JSONField()
