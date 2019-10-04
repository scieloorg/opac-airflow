FROM scieloorg/airflow:1.10.4

ARG AIRFLOW_HOME=/usr/local/airflow
ARG PROC_DIR=/usr/local/proc

# Custom Airflow
COPY --chown=airflow:airflow ./airflow ${AIRFLOW_HOME}
COPY --chown=airflow:airflow ./proc ${PROC_DIR}

USER root
RUN chmod +x ${PROC_DIR}/*

RUN apk add --no-cache --virtual .build-deps \
        make gcc g++ libstdc++ libxml2-dev libxslt-dev \
    && apk add libxml2 libxslt curl postgresql-dev \
    && pip install --no-cache-dir https://git@github.com/scieloorg/opac_schema/archive/v2.52.tar.gz \
    && pip install --no-cache-dir xylose==1.35.1 \
    && pip install --no-cache-dir psycopg2-binary \
    && pip install --no-cache-dir 'deepdiff[murmur]' \
    && pip install --no-cache-dir lxml==4.3.4 \
    && apk --purge del .build-deps

COPY ./entrypoint.sh /entrypoint.sh
COPY ./start_airflow.sh /start_airflow.sh

RUN chmod +x /entrypoint.sh
RUN chmod +x /start_airflow.sh

USER airflow

ENTRYPOINT ["/entrypoint.sh"]
