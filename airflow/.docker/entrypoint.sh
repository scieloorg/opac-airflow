#!/usr/bin/env bash

case "$1" in
  webserver)
    airflow initdb
    airflow scheduler &
    airflow webserver
    ;;

  worker|scheduler|version)
    exec airflow "$@"
    ;;

  *)
    exec "$@"
    ;;
esac
