# kernel_changes

É responsável, atualmente, pela identificação das alterações realizadas nos metadados do SciELO e pela consistência no Site(OPAC).

## Premissas que o processamento deve contemplar

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
