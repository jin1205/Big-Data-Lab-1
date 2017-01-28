# -*- coding: utf-8 -*-
# Generated by Django 1.10.3 on 2016-11-23 05:35
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('recommend', '0001_initial'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='stores',
            name='citySeansonInfo',
        ),
        migrations.AddField(
            model_name='stores',
            name='city',
            field=models.CharField(default='city', max_length=200),
        ),
        migrations.AddField(
            model_name='stores',
            name='season',
            field=models.CharField(default='season', max_length=200),
        ),
        migrations.AlterField(
            model_name='stores',
            name='storeID',
            field=models.CharField(default='0', max_length=200),
        ),
        migrations.AlterField(
            model_name='stores',
            name='storeName',
            field=models.CharField(default='store', max_length=200),
        ),
    ]