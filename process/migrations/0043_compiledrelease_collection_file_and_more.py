# Generated by Django 4.2.15 on 2024-11-02 03:30

import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("process", "0042_compiledrelease_release_date_release_release_date"),
    ]

    operations = [
        migrations.AddField(
            model_name="compiledrelease",
            name="collection_file",
            field=models.ForeignKey(
                db_index=False, null=True, on_delete=django.db.models.deletion.CASCADE, to="process.collectionfile"
            ),
        ),
        migrations.AddField(
            model_name="record",
            name="collection_file",
            field=models.ForeignKey(
                db_index=False, null=True, on_delete=django.db.models.deletion.CASCADE, to="process.collectionfile"
            ),
        ),
        migrations.AddField(
            model_name="release",
            name="collection_file",
            field=models.ForeignKey(
                db_index=False, null=True, on_delete=django.db.models.deletion.CASCADE, to="process.collectionfile"
            ),
        ),
        migrations.AddIndex(
            model_name="compiledrelease",
            index=models.Index(fields=["collection_file"], name="compiled_release_collection_file_id_idx"),
        ),
        migrations.AddIndex(
            model_name="record",
            index=models.Index(fields=["collection_file"], name="record_collection_file_id_idx"),
        ),
        migrations.AddIndex(
            model_name="release",
            index=models.Index(fields=["collection_file"], name="release_collection_file_id_idx"),
        ),
        migrations.AlterField(
            model_name="compiledrelease",
            name="collection_file_item",
            field=models.ForeignKey(
                db_index=False, null=True, on_delete=django.db.models.deletion.CASCADE, to="process.collectionfileitem"
            ),
        ),
        migrations.AlterField(
            model_name="record",
            name="collection_file_item",
            field=models.ForeignKey(
                db_index=False, null=True, on_delete=django.db.models.deletion.CASCADE, to="process.collectionfileitem"
            ),
        ),
        migrations.AlterField(
            model_name="release",
            name="collection_file_item",
            field=models.ForeignKey(
                db_index=False, null=True, on_delete=django.db.models.deletion.CASCADE, to="process.collectionfileitem"
            ),
        ),
        migrations.RunSQL(
            """
            UPDATE release
            SET collection_file_id = collection_file_item.collection_file_id
            FROM collection_file_item
            WHERE collection_file_item_id = collection_file_item.id
            """
        ),
        migrations.RunSQL(
            """
            UPDATE record
            SET collection_file_id = collection_file_item.collection_file_id
            FROM collection_file_item
            WHERE collection_file_item_id = collection_file_item.id
            """
        ),
        migrations.RunSQL(
            """
            UPDATE compiled_release
            SET collection_file_id = collection_file_item.collection_file_id
            FROM collection_file_item
            WHERE collection_file_item_id = collection_file_item.id
            """
        ),
    ]