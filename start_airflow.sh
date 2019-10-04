#!/bin/sh

airflow initdb
airflow scheduler
airflow webserver
