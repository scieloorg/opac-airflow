if [ -f SyncToKernel.ini ];
then
    echo "VARIABLES read from file SyncToKernel.ini"
    . SyncToKernel.ini
fi

if [ "" != "${OPAC_AIRFLOW}" ];
then
	curl -X POST \
      ${OPAC_AIRFLOW}/api/experimental/dags/sync_isis_to_kernel/dag_runs \
      -H 'Cache-Control: no-cache' \
      -H 'Content-Type: application/json'
fi
