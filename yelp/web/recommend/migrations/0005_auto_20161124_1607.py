# -*- coding: utf-8 -*-
# Generated by Django 1.10.3 on 2016-11-25 00:07
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('recommend', '0004_auto_20161124_1434'),
    ]

    operations = [
        migrations.CreateModel(
            name='UserRatings',
            fields=[
                ('user', models.CharField(max_length=200)),
                ('rating', models.CharField(max_length=200)),
                ('store_id', models.CharField(default='0', max_length=200, primary_key=True, serialize=False)),
            ],
        ),
        migrations.DeleteModel(
            name='CitySeansonInfo',
        ),
    ]
