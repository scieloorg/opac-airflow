if [ -f SyncToKernel.ini ];
then
    echo "VARIABLES read from file SyncToKernel.ini"
    . SyncToKernel.ini
fi

if [ "" == "${OPAC_AIRFLOW}" ];
then
    echo "MISSING OPAC_AIRFLOW"
else
    URL="${OPAC_AIRFLOW}/api/experimental/dags/sync_isis_to_kernel/dag_runs"
    echo "curl -X POST ${URL} -H 'Cache-Control: no-cache' -H 'Content-Type: application/json' -d '{}' -v"
    curl -X POST \
        ${URL} \
        -H 'Cache-Control: no-cache' \
        -H 'Content-Type: application/json' \
        -d '{}' \
        -v
fi
