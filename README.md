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

## Conexão com Object Store (Minio):

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
* `NEW_SPS_ZIP_DIR`: Diretório de destino dos pacotes SPS otimizados
* `WEBSITE_URL_LIST`: Lista de URL de SciELO Website para validar a disponibilidade de recursos. Exemplo: ["http://www.scielo.br", "https://new.scielo.br"]
* `OBJECT_STORE_URL`: URL do Object Store para filtrar os URI existentes nos HTML para que sejam usados na verificação de presença/ausência de menção dos ativos digitais e manifestações do documento no código HTML
* `KERNEL_FETCH_DATA_TIMEOUT`: Timeout para requisições de leitura do Kernel



## Variáveis opcionais:
* `TIMEOUT_FOR_SINGLE_REQ` (opcional): informa um valor inteiro em segundos para o _timeout_ de uma requisição. Valor padrão 10 segundos
* `TIMEOUT_FOR_MULT_REQ` (opcional): informa um valor inteiro em segundos para o _timeout_ total de requisições simultâneas. Valor padrão: 300 segundos
* `PID_LIST_CSV_FILE_NAMES` (opcional): Lista de nomes de arquivos CSV que deverão estar presentes em `XC_SPS_PACKAGES_DIR` para que sejam copiados para `PROC_SPS_PACKAGES_DIR`/`DAG_RUN_ID`, e usados na DAG de verificação da disponibilidade dos documentos, ativos digitais e manifestações
* `CHECK_SCI_SERIAL_PAGES` (opcional, valor padrão é `true`): com valor `false` inibe a execução da verificação das páginas `sci_serial`
* `CHECK_SCI_ISSUES_PAGES` (opcional, valor padrão é `true`): com valor `false` inibe a execução da verificação das páginas `sci_issues`
* `CHECK_SCI_ISSUETOC_PAGES` (opcional, valor padrão é `true`): com valor `false` inibe a execução da verificação das páginas `sci_issuetoc`
* `CHECK_SCI_ARTTEXT_PAGES` (opcional, valor padrão é `true`): com valor `false` inibe a execução da verificação das páginas `sci_arttext`
* `CHECK_SCI_PDF_PAGES` (opcional, valor padrão é `true`): com valor `false` inibe a execução da verificação das páginas `sci_pdf`
* `CHECK_RENDITIONS` (opcional, valor padrão é `true`): com valor `false` inibe a execução da verificação das manifestações, tal como estão registradas no _Object store_
* `CHECK_DIGITAL_ASSETS` (opcional, valor padrão é `true`): com valor `false` inibe a execução da verificação dos ativos digitais, tal como estão registrados no _Object store_
* `CHECK_WEB_HTML_PAGES` (opcional, valor padrão é `true`): com valor `false` inibe a execução da verificação das páginas Web do padrão `/j/:acron/a/:iddoc/format=html&lang=??`, com as variações de idioma. Também inibiria a verficação dos componentes do HTML resultante desta consulta, ou seja, a presença dos ativos digitais e a presença dos links para as demais versões do documento (formato e idioma)
* `CHECK_WEB_PDF_PAGES` (opcional, valor padrão é `true`): inibe a execução da verificação das páginas Web do padrão `/j/:acron/a/:iddoc/format=pdf&lang=??`, com as variações de idioma
* `IS_SPORADIC`: A boolean to define if must send e-mail with the finished of the flow. Default value is ``False``, possible values ['False', 'True'] all strings.


## Variáveis de ambiente:

* `AIRFLOW_HOME`: Diretório de instalação da aplicação
* `EMIAL_ON_FAILURE_RECIPIENTS`: Conta de e-mail para envio de falha, padrão: infra@scielo.org
* `AIRFLOW__SMTP__SMTP_HOST`: Endereço do servidor de e-mail
* `AIRFLOW__SMTP__SMTP_USER`: Endereço de e-mail responsável pelo envio de e-mails
* `AIRFLOW__SMTP__SMTP_PASSWORD`: Endereço de e-mail responsável pelo envio de e-mails
* `AIRFLOW__SMTP__SMTP_MAIL_FROM`: Endereço de e-mail do remetente
* `AIRFLOW__SMTP__SMTP_SSL`: ```True``` ou ```False``` para indicar o uso de criptografia no servidor de e-mail
* `AIRFLOW__SMTP__SMTP_PORT`: Porta do servidor de e-mail
* `AIRFLOW__SENTRY__SENTRY_DSN`: DSN do projeto cadastrado no Sentry para logar tracebacks registrados nas execuções
* `POSTGRES_USER`: Usuário para conexão com o Postgres
* `POSTGRES_PASSWORD`: Senha para conexão com o Postgres
* `POSTGRES_HOST`: Endereço do Postgres
* `POSTGRES_PORT`: Porta de rede para conexão com o Postgres
* `POSTGRES_DB`: Nome do banco de dados do Opac-airflow para conexão com Postgres


## Testes Automatizados

No Docker:
`docker-compose -f docker-compose-dev.yml exec opac-airflow python -m unittest -v`

No servidor local:

Dentro da pasta **opac-airflow**, executar:


```
export AIRFLOW_HOME=$(pwd)/airflow
export AIRFLOW__CORE__DAGS_FOLDER=$(pwd)/airflow/dags
export AIRFLOW__CORE__BASE_LOG_FOLDER=$(pwd)/airflow/logs
export AIRFLOW__CORE__DAG_PROCESSOR_MANAGER_LOG_LOCATION=$(pwd)/airflow/logs/dag_processor_manager/dag_processor_manager.log
export AIRFLOW__CORE__PLUGINS_FOLDER=$(pwd)/airflow/plugins
export AIRFLOW__SCHEDULER__CHILD_PROCESS_LOG_DIRECTORY=$(pwd)/airflow/logs/scheduler
export POSTGRES_USER=username
export POSTGRES_PASSWORD=''
export POSTGRES_HOST=localhost
export POSTGRES_PORT=5432
export POSTGRES_DB=airflow_externo

cd airflow
python -m unittest -v
```


## Licença de uso

Copyright 2018 SciELO <scielo-dev@googlegroups.com>. Licensed under the terms
of the BSD license. Please see LICENSE in the source code for more
information.

https://github.com/scieloorg/opac-airflow/blob/master/LICENSE
