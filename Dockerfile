FROM scieloorg/airflow:1.10.4

ARG AIRFLOW_HOME=/usr/local/airflow

# Custom Airflow
COPY --chown=airflow:airflow ./airflow ${AIRFLOW_HOME}

USER root
RUN apk add --no-cache --virtual .build-deps \
        make gcc g++ libstdc++ libxml2-dev libxslt-dev \
    && apk add libxml2 libxslt \
    && pip install --no-cache-dir https://git@github.com/scieloorg/opac_schema/archive/v2.52.tar.gz \
    && pip install --no-cache-dir xylose==1.35.1 \
    && pip install --no-cache-dir 'deepdiff[murmur]' \
    && pip install --no-cache-dir lxml==4.3.4 \
    && apk --purge del .build-deps

USER airflow
