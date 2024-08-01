#!/usr/bin/env bash

airflow db init
airlfow db upgrade
airflow users create -r Admin -u admin -p admin -e admin@example.com -f admin -l airflow
airflow scheduler &
airflow webserver