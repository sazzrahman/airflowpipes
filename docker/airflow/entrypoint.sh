#!/usr/bin/env bash

# initialize the metastore
airflow db init

# Run the scheduler in background
airflow scheduler &> /dev/null &

airflow users create -u admin -p admin -r Admin -e admin@admin.com -f admin -l admin

exec airflow webserver