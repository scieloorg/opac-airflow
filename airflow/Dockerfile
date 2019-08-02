FROM python:3.5-alpine

# Airflow config

ARG AIRFLOW_HOME=/usr/local/airflow
ENV AIRFLOW_GPL_UNIDECODE yes

WORKDIR ${AIRFLOW_HOME}

COPY requirements.txt ${AIRFLOW_HOME}

RUN apk add --no-cache --virtual .build-deps \
        make gcc libxml2-dev libxslt-dev musl-dev g++ \
        python3-dev libstdc++ linux-headers \
    && apk add libxml2 libxslt sudo bash git openjdk8 \
    && ln -s /usr/include/locale.h /usr/include/xlocale.h

RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir numpy==1.16.2 \
    && pip install --no-cache-dir pandas==0.24.1 \
    && pip install --no-cache-dir -r requirements.txt \
    && pip install --no-cache-dir apache-airflow==01.10.2 \
    && pip install --no-cache-dir -e git+https://git@github.com/scieloorg/opac_schema@v2.52#egg=opac_schema

RUN pip install --no-cache-dir xylose==1.35.1 \
    && pip install --no-cache-dir 'deepdiff[murmur]' \
    && addgroup -S airflow \
    && adduser -S airflow -G airflow -h ${AIRFLOW_HOME} \
    && apk --purge del .build-deps

ENV JAVA_HOME=/usr/lib/jvm/java-1.8-openjdk
ENV PATH="$JAVA_HOME/bin:${PATH}"

EXPOSE 8080

COPY . ${AIRFLOW_HOME}
RUN chown -R airflow:airflow ${AIRFLOW_HOME}

USER airflow

ENTRYPOINT ["/usr/local/airflow/.docker/entrypoint.sh"]
