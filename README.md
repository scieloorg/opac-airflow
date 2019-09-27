# OPAC-Airflow

OPAC-Airflow é a configuração SciELO do [Apache Airflow](http://airflow.apache.org/) para o controle do fluxo de metadados e documentos para a publicação no Site(OPAC), desde o fluxo direto de ingestão legado.

[![Build Status](https://travis-ci.org/scieloorg/opac-airflow.svg?branch=master)](https://travis-ci.org/scieloorg/opac-airflow)


## Funcionamento

Os metadados são armazenados no [Kernel](https://github.com/scieloorg/document-store) e os arquivos (XMLs, ativos digitais e manifestações) no [Minio](https://min.io/) (também compatível com AWS S3). Para que os dados sejam carregados no site [OPAC](https://github.com/scieloorg/OPAC), são lidos os registros de mudança no Kernel.

### Responsabilidades de cada DAG

* `kernel_gate`: efetua o espelhamento das bases ISIS no Kernel
* `pre_sync_documents_to_kernel`: copia os pacotes SPS informados
* `sync_documents_to_kernel`: sincroniza com o Kernel e o Minio os documentos de um pacote SPS
* `kernel_changes`: carrega os metadados do Kernel no site


## Instalação

## Construindo a aplicação com Docker

`docker-compose build`

Executando a aplicação:

`docker-compose up -d`

## Instalação no servidor

### Requisitos

* Python 3.7+
* [Apache Airflow com S3](http://airflow.apache.org/installation.html)
* [OpenJDK8](https://openjdk.java.net)
* [Xylose 1.35.1](https://github.com/scieloorg/xylose)
* [OPAC-Schema 2.52](https://github.com/scieloorg/opac_schema)
* [deepdiff com Murmur3](https://deepdiff.readthedocs.io/)
* [lxml 4.3.4](https://lxml.de)

### Inicializando o servidor

```
$ airflow initdb
$ airflow scheduler
$ airflow webserver
```

## Configuração

### Airflow

## Conexão com OPAC:

* Conn Id: `opac_conn`
* Conn Type: `MongoDB`
* Host: endereço do host MongoDB
* Schema: `opac`
* Port: porta do host MongoDB
* Extra: `{"authentication_source": "admin"}`

## Conexão com Kernel:

* Conn Id: `kernel_conn`
* Conn Type: `HTTP`
* Host: endereço do host do Kernel
* Port: porta do host do Kernel

## Conexão com Object Store (Min.io):

* Conn Id: `aws_default`
* Conn Type: `Amazon Web Service`
* Schema: `http` ou `https`
* Login: login do Object Store
* Extra: `{"host": "<endereço do host:porta>"}`

## Variáveis:

* `BASE_TITLE_FOLDER_PATH`: Diretório de origem da base ISIS title
* `BASE_ISSUE_FOLDER_PATH`: Diretório de origem da base ISIS issue
* `WORK_FOLDER_PATH`: Diretório para a cópia das bases ISIS
* `SCILISTA_FILE_PATH`: Caminho onde o arquivo `scilista` deverá ser lido
* `XC_SPS_PACKAGES_DIR`: Diretório de origem dos pacotes SPS a serem sincronizados
* `PROC_SPS_PACKAGES_DIR`: Diretório de destino dos pacotes SPS a serem sincronizados


## Variáveis de ambiente:

* `AIRFLOW_HOME`: Diretório de instalação da aplicação 
* `EMIAL_ON_FAILURE_RECIPIENTS`: Conta de e-mail para envio de falha, padrão: infra@scielo.org
* `AIRFLOW__SMTP__SMTP_HOST`: Endereço do servidor de e-mail
* `AIRFLOW__SMTP__SMTP_USER`: Endereço de e-mail responsável pelo envio de e-mails
* `AIRFLOW__SMTP__SMTP_PASSWORD`: Endereço de e-mail responsável pelo envio de e-mails
* `AIRFLOW__SMTP__SMTP_MAIL_FROM`: Endereço de e-mail do remetente
* `AIRFLOW__SMTP__SMTP_SSL`: ```True``` ou ```False``` para indicar o uso de criptografia no servidor de e-mail
* `AIRFLOW__SMTP__SMTP_PORT`: Porta do servidor de e-mail


## Testes Automatizados

No Docker:
`docker-compose -f docker-compose-dev.yml exec opac-airflow python -m unittest -v`

No servidor local:
`python -m unittest -v`

## Licença de uso

Copyright 2018 SciELO <scielo-dev@googlegroups.com>. Licensed under the terms
of the BSD license. Please see LICENSE in the source code for more
information.

https://github.com/scieloorg/opac-airflow/blob/master/LICENSE
