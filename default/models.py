from django.db import models
from django.contrib.postgres.fields import JSONField

# # We set `db_table` so that the table names are identical to those created by SQLAlchemy in an earlier version.
#
# We don't use `unique=True` or `db_index=True`, because they create an additional index for the text fields `hash_md5`
# and `ocid`. Instead, we set `Meta.constraints` and `Meta.indexes`.
#
# We don't use default index names (including for foreign key fields) or `%(class)s` in unique constraint names -
# we are explicit, instead - so that the names are identical to those created by SQLAlchemy in an earlier version.
# Otherwise, Django will create a migration to change the name of the index or constraint.

class Collection(models.Model):
    """
    A collection of data from a source.
    """
    class Meta:
        db_table = 'collection'
        constraints = [
            models.UniqueConstraint(name='unique_collection_identifiers', fields=[
                'source_id', 'data_version', 'sample', 'transform_from_collection', 'transform_type']),
        ]

    # Identification
    source_id = models.TextField()
    data_version = models.DateTimeField()

    # Routing slip
    sample = models.BooleanField(default=False)
    check_data = models.BooleanField(default=False)
    check_older_data_with_schema_version_1_1 = models.BooleanField(default=False)

    # Provenance
    transform_from_collection = models.ForeignKey('self', on_delete=models.CASCADE, null=True, blank=True,
                                                  db_index=False)
    transform_type = models.TextField(null=True, blank=True)

    # Calculated fields
    cached_releases_count = models.IntegerField(null=True, blank=True)
    cached_records_count = models.IntegerField(null=True, blank=True)
    cached_compiled_releases_count = models.IntegerField(null=True, blank=True)

    # Lifecycle
    store_start_at = models.DateTimeField()
    store_end_at = models.DateTimeField(null=True, blank=True)
    deleted_at = models.DateTimeField(null=True, blank=True)


class CollectionNote(models.Model):
    """
    A note an analyst made about the collection.
    """
    class Meta:
        db_table = 'collection_note'

    collection = models.ForeignKey(Collection, on_delete=models.CASCADE, db_index=False)
    note = models.TextField()
    stored_at = models.DateTimeField()


class CollectionFile(models.Model):
    """
    A file within the collection.
    """
    class Meta:
        db_table = 'collection_file'
        constraints = [
            models.UniqueConstraint(name='unique_collection_file_identifiers', fields=[
                'collection', 'filename']),
        ]

    collection = models.ForeignKey(Collection, on_delete=models.CASCADE, db_index=False)

    filename = models.TextField(null=True, blank=True)
    url = models.TextField(null=True, blank=True)

    warnings = JSONField(null=True, blank=True)
    errors = JSONField(null=True, blank=True)

    store_start_at = models.DateTimeField(null=True, blank=True)
    store_end_at = models.DateTimeField(null=True, blank=True)


class CollectionFileItem(models.Model):
    """
    An item within a file in the collection.
    """
    class Meta:
        db_table = 'collection_file_item'
        constraints = [
            models.UniqueConstraint(name='unique_collection_file_item_identifiers', fields=[
                'collection_file', 'number']),
        ]

    collection_file = models.ForeignKey(CollectionFile, on_delete=models.CASCADE, db_index=False)

    number = models.IntegerField(null=True, blank=True)

    warnings = JSONField(null=True, blank=True)
    errors = JSONField(null=True, blank=True)

    store_start_at = models.DateTimeField(null=True, blank=True)
    store_end_at = models.DateTimeField(null=True, blank=True)


class Data(models.Model):
    """
    The contents of a release, record or compiled release.
    """
    class Meta:
        db_table = 'data'
        constraints = [
            models.UniqueConstraint(name='unique_data_hash_md5', fields=['hash_md5']),
        ]

    hash_md5 = models.TextField()
    data = JSONField()


class PackageData(models.Model):
    """
    The contents of a package, excluding the releases or records.
    """
    class Meta:
        db_table = 'package_data'
        constraints = [
            models.UniqueConstraint(name='unique_package_data_hash_md5', fields=['hash_md5']),
        ]

    hash_md5 = models.TextField()
    data = JSONField()


class Release(models.Model):
    """
    A release.
    """
    class Meta:
        db_table = 'release'
        indexes = [
            models.Index(name='release_collection_file_item_id_idx', fields=['collection_file_item']),
            models.Index(name='release_ocid_idx', fields=['ocid']),
            models.Index(name='release_data_id_idx', fields=['data']),
        ]

    collection_file_item = models.ForeignKey(CollectionFileItem, on_delete=models.CASCADE, db_index=False)

    release_id = models.TextField(null=True, blank=True)
    ocid = models.TextField(null=True, blank=True)

    data = models.ForeignKey(Data, on_delete=models.CASCADE, db_index=False)
    package_data = models.ForeignKey(PackageData, on_delete=models.CASCADE, db_index=False)


class Record(models.Model):
    """
    A record.
    """
    class Meta:
        db_table = 'record'
        indexes = [
            models.Index(name='record_collection_file_item_id_idx', fields=['collection_file_item']),
            models.Index(name='record_ocid_idx', fields=['ocid']),
            models.Index(name='record_data_id_idx', fields=['data']),
        ]

    collection_file_item = models.ForeignKey(CollectionFileItem, on_delete=models.CASCADE, db_index=False)

    ocid = models.TextField(null=True, blank=True)

    data = models.ForeignKey(Data, on_delete=models.CASCADE, db_index=False)
    package_data = models.ForeignKey(PackageData, on_delete=models.CASCADE, db_index=False)


class CompiledRelease(models.Model):
    """
    A compiled release.
    """
    class Meta:
        db_table = 'compiled_release'
        indexes = [
            models.Index(name='compiled_release_collection_file_item_id_idx', fields=['collection_file_item']),
            models.Index(name='compiled_release_ocid_idx', fields=['ocid']),
            models.Index(name='compiled_release_data_id_idx', fields=['data']),
        ]

    collection_file_item = models.ForeignKey(CollectionFileItem, on_delete=models.CASCADE, db_index=False)

    ocid = models.TextField(null=True, blank=True)

    data = models.ForeignKey(Data, on_delete=models.CASCADE, db_index=False)


class ReleaseCheck(models.Model):
    """
    The result of checking a release.
    """
    class Meta:
        db_table = 'release_check'
        constraints = [
            models.UniqueConstraint(name='unique_release_check_release_id_and_more', fields=[
                'release', 'override_schema_version']),
        ]

    release = models.ForeignKey(Release, on_delete=models.CASCADE, db_index=False)
    override_schema_version = models.TextField(null=True, blank=True)
    cove_output = JSONField()


class RecordCheck(models.Model):
    """
    The result of checking a record.
    """
    class Meta:
        db_table = 'record_check'
        constraints = [
            models.UniqueConstraint(name='unique_record_check_record_id_and_more', fields=[
                'record', 'override_schema_version']),
        ]

    record = models.ForeignKey(Record, on_delete=models.CASCADE, db_index=False)
    override_schema_version = models.TextField(null=True, blank=True)
    cove_output = JSONField()


class ReleaseCheckError(models.Model):
    class Meta:
        db_table = 'release_check_error'
        constraints = [
            models.UniqueConstraint(name='unique_release_check_error_release_id_and_more', fields=[
                'release', 'override_schema_version']),
        ]

    release = models.ForeignKey(Release, on_delete=models.CASCADE, db_index=False)
    override_schema_version = models.TextField(null=True, blank=True)
    error = models.TextField()


class RecordCheckError(models.Model):
    class Meta:
        db_table = 'record_check_error'
        constraints = [
            models.UniqueConstraint(name='unique_record_check_error_record_id_and_more', fields=[
                'record', 'override_schema_version']),
        ]

    record = models.ForeignKey(Record, on_delete=models.CASCADE, db_index=False)
    override_schema_version = models.TextField(null=True, blank=True)
    error = models.TextField()
