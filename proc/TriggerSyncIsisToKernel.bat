GERAPADRAO_ID=$1

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
    CONF="{\"conf\": {\"GERAPADRAO_ID\": \"${GERAPADRAO_ID}\"} }"

    echo "curl -X POST ${URL} -H 'Cache-Control: no-cache' -H 'Content-Type: application/json' -d '${CONF}' -v"
    curl -X POST \
        ${URL} \
        -H 'Cache-Control: no-cache' \
        -H 'Content-Type: application/json' \
        -d "${CONF}" \
        -v
fi
