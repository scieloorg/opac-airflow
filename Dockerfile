FROM scieloorg/airflow:1.10.12

ARG AIRFLOW_HOME=/usr/local/airflow
ARG PROC_DIR=/usr/local/proc

# Custom Airflow
COPY --chown=airflow:airflow ./airflow ${AIRFLOW_HOME}
COPY --chown=airflow:airflow ./requirements.txt ${AIRFLOW_HOME}
COPY --chown=airflow:airflow ./proc ${PROC_DIR}

USER root
RUN chmod +x ${PROC_DIR}/*
RUN pip install --upgrade pip

RUN apk add --no-cache --virtual .build-deps \
        make gcc g++ libstdc++ libxml2-dev libxslt-dev jpeg-dev zlib-dev \
    && apk add libxml2 libxslt curl postgresql-dev tiff tiff-dev \
    && pip install --no-cache-dir -r requirements.txt \
    && apk --purge del .build-deps

COPY ./entrypoint.sh /entrypoint.sh
COPY ./scripts /scripts

RUN chmod +x /entrypoint.sh
RUN chmod +x /scripts/start_webserver.sh
RUN chmod +x /scripts/start_scheduler_autorestart.sh

USER airflow

ENTRYPOINT ["/usr/local/bin/dumb-init", "/entrypoint.sh"]
