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

# CORREÇÃO: Adicionar dependências lcms2 para Pillow > 6.0 (packtools >= 2.9.5) color management
# Isso resolve o erro "_imagingcms ImportError" quando packtools processa imagens
RUN apk add --no-cache --virtual .build-deps \
        make gcc g++ libstdc++ libxml2-dev libxslt-dev jpeg-dev zlib-dev \
        lcms2-dev freetype-dev openjpeg-dev tiff-dev libpng-dev \
    && apk add --no-cache \
        libxml2 libxslt curl postgresql-dev tiff tiff-dev \
        lcms2 freetype openjpeg libpng \
    && pip install --no-cache-dir -r requirements.txt \
    && apk --purge del .build-deps

COPY ./entrypoint.sh /entrypoint.sh
COPY ./start_airflow.sh /start_airflow.sh

RUN chmod +x /entrypoint.sh
RUN chmod +x /start_airflow.sh

USER airflow

ENTRYPOINT ["/usr/local/bin/dumb-init", "/entrypoint.sh"]
