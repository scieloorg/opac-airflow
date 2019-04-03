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

## Implantação local

Variáveis de ambiente:

```
OPAC_MONGODB_NAME
OPAC_MONGODB_HOSTNAME
OPAC_MONGODB_USERNAME
OPAC_MONGODB_PASS
```

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


## Licença de uso

Copyright 2018 SciELO <scielo-dev@googlegroups.com>. Licensed under the terms
of the BSD license. Please see LICENSE in the source code for more
information.

https://github.com/scieloorg/opac-airflow/blob/master/LICENSE
