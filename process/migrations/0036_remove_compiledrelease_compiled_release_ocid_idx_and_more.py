# Generated by Django 4.2.11 on 2024-04-18 04:04

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("process", "0035_remove_processingstep_processing_step_collection_file_id_idx_and_more"),
    ]

    operations = [
        migrations.RemoveIndex(
            model_name="compiledrelease",
            name="compiled_release_ocid_idx",
        ),
        migrations.RemoveIndex(
            model_name="record",
            name="record_ocid_idx",
        ),
        migrations.RemoveIndex(
            model_name="release",
            name="release_ocid_idx",
        ),
        migrations.AddIndex(
            model_name="compiledrelease",
            index=models.Index(fields=["collection", "ocid"], name="compiled_re_collect_0adec3_idx"),
        ),
        migrations.AddIndex(
            model_name="record",
            index=models.Index(fields=["collection", "ocid"], name="record_collect_65843f_idx"),
        ),
        migrations.AddIndex(
            model_name="release",
            index=models.Index(fields=["collection", "ocid"], name="release_collect_4cb073_idx"),
        ),
    ]