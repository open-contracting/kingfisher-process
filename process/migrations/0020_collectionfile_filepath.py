# Generated by Django 3.0.4 on 2021-03-08 14:53

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('process', '0019_auto_20210217_1355'),
    ]

    operations = [
        migrations.AddField(
            model_name='collectionfile',
            name='filepath',
            field=models.TextField(blank=True),
        ),
    ]