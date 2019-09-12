# Opac-airflow

Opac-airflow é uma plataforma de fluxo de processamento dos metadados do SciELO para o Site(OPAC).

É responsável, atualmente, pela identificação das alterações realizadas nos metadados do SciELO e pela consistência no Site(OPAC).


# Premissas que o processamento deve contemplar

* Deve coletar somente as mudanças
* Verificar antecipadamente os pontos de integração
* Enviar e-mail em caso de falha
* Cadastrar os registros sempre como despublicados, até que todo as tarefas sejam finalizadas
* Realizar as tarefas de remoção nos últimos passos se e somente se todos os
passos tenham sido realizados com sucesso
* Ao finalizar com sucesso, enviar e-mail com um relatório de processamento
contento:

    * Quais itens foram inseridos e removidos
    * Tabela de links para os novos itens cadastrados para validação manual
    * Tempo de execução total e por tarefa
    * Link para airflow com o contextualizado processamento.


## Fluxo


## Requisitos

* Python 3.7+
* Apache Airflow
* git+https://git@github.com/scieloorg/opac_schema@v2.52#egg=opac_schema
* curl

## Implantação local

### Conexões do airflow:


#### Conexão com OPAC:

* Conn Id: opac_conn
* Conn Type: MongoDB
* Host: localhost
* Schema: opac
* Port: 27017
* Extra: {"authentication_source": "admin"}

#### Conexão com Kernel:

* Conn Id: kernel_conn
* Conn Type: HTTP
* Host: http://0.0.0.0
* Port: 6543


### Variáveis de ambiente:

```
export AIRFLOW_HOME="$(pwd)"
```

```
airflow initdb
```

```
airflow scheduler
```

```
airflow webserver --port=8080
```

Construindo a aplicação com docker:

```docker-compose build```

Executando a aplicação:

```docker-compose up -d```

### Teste

```
docker-compose -f docker-compose-dev.yml exec opac-airflow python -m unittest -v
```

## Licença de uso

Copyright 2018 SciELO <scielo-dev@googlegroups.com>. Licensed under the terms
of the BSD license. Please see LICENSE in the source code for more
information.

https://github.com/scieloorg/opac-airflow/blob/master/LICENSE
