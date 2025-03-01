# Generated by Django 3.0.4 on 2021-02-17 12:33

import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("process", "0015_processingstep_collection"),
    ]

    operations = [
        migrations.AlterField(
            model_name="processingstep",
            name="collection",
            field=models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to="process.Collection"),
        ),
        migrations.AlterField(
            model_name="processingstep",
            name="collection_file",
            field=models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to="process.CollectionFile"),
        ),
    ]
