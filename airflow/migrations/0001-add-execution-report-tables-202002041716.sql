/*
* A tabela xml_documents guarda o registro de iterações dos xmls que estão dentro 
* de um pacote SPS.
* 
* Para cada artigo registrado ou deletado durante a sincronização, nós adicionamos
* uma entrada nesta tabela.
*/ 
CREATE TABLE IF NOT EXISTS xml_documents (
  id SERIAL PRIMARY KEY,
  pid varchar(23) NULL,                             -- scielo-pid-v3 presente no xml
  package_name varchar(255) null,                   -- Nome do pacote processado ex: `rsp_v53.zip`
  dag_run varchar(255),                             -- Identificador de execução da dag `sync_documents_to_kernel`
  pre_sync_dag_run varchar(255) NULL,               -- Identificador de execução da dag `pre_sync_documents_to_kernel` 
  deletion bool DEFAULT false,
  file_name varchar(255),                           -- Nome do arquivo xml
  failed bool DEFAULT false,
  error text null,                                  -- Erro lançado durante processamento
  payload json,                                     -- Dado enviado para o Kernel
  created_at timestamptz DEFAULT now()
);

CREATE INDEX IF NOT EXISTS xml_documents_pid ON xml_documents (pid);
CREATE INDEX IF NOT EXISTS xml_documents_dag_run ON xml_documents (dag_run);
CREATE INDEX IF NOT EXISTS xml_documents_package_name ON xml_documents (package_name);
CREATE INDEX IF NOT EXISTS xml_documents_pre_sync_dag_run ON xml_documents (pre_sync_dag_run);

/*
* A tabela xml_documentsbundle guarda o registro de relacionamento entre
* os artigos e seus respectivos `documentsbundle`.
*/
CREATE TABLE IF NOT EXISTS xml_documentsbundle (
    id SERIAL PRIMARY KEY,
    pid varchar(23) NULL,
    bundle_id varchar(100),
    package_name varchar(255) null,
    dag_run varchar(255),
    pre_sync_dag_run varchar(255) NULL,
    ex_ahead bool default false,
    removed bool default false,
    failed bool DEFAULT false,
    error text null,
    payload json null,
    created_at timestamptz default now()
);

CREATE INDEX IF NOT EXISTS xml_documentsbundle_pid ON xml_documentsbundle (pid);
CREATE INDEX IF NOT EXISTS xml_documentsbundle_dag_run ON xml_documentsbundle (dag_run);
CREATE INDEX IF NOT EXISTS xml_documentsbundle_package_name ON xml_documentsbundle (package_name);
CREATE INDEX IF NOT EXISTS xml_documentsbundle_pre_sync_dag_run ON xml_documentsbundle (pre_sync_dag_run);