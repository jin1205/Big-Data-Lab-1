# -*- coding: utf-8 -*-
# Generated by Django 1.10.3 on 2016-11-24 22:34
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('recommend', '0003_auto_20161122_2206'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='stores',
            name='id',
        ),
        migrations.AlterField(
            model_name='stores',
            name='store_id',
            field=models.CharField(default='0', max_length=200, primary_key=True, serialize=False),
        ),
    ]
