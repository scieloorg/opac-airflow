/*
  A tabela sci_pages_availability guarda o registro do resultado da verificação
  da disponibilidade de URI do padrão do site clássico, pois o site novo deve
  continuar a atender o padrão anterior
  Para cada URI verificada, é criada uma nova entrada, mesmo que haja
  reexecução da DAG
*/ 
CREATE TABLE IF NOT EXISTS sci_pages_availability (
  id SERIAL PRIMARY KEY,
  dag_run varchar(255),                             -- Identificador de execução da dag `check_website`
  input_file_name varchar(255) NULL,                -- Nome do arquivo de entrada: csv com PIDs v2 ou uri_list
  uri varchar(255),                                 -- URI
  failed bool DEFAULT false,                        -- Falha == true
  detail json,                                      -- Detalhes da verificação da disponibilidade
  pid_v2_journal varchar(9),                        -- ISSN ID do periódico
  pid_v2_issue varchar(17) NULL,                    -- PID do fascículo
  pid_v2_doc varchar(23) NULL,                      -- PID do documento
  created_at timestamptz DEFAULT now()
);

CREATE INDEX IF NOT EXISTS sci_pages_availability_pid_v2_journal ON sci_pages_availability (pid_v2_journal);
CREATE INDEX IF NOT EXISTS sci_pages_availability_pid_v2_issue ON sci_pages_availability (pid_v2_issue);
CREATE INDEX IF NOT EXISTS sci_pages_availability_pid_v2_doc ON sci_pages_availability (pid_v2_doc);
CREATE INDEX IF NOT EXISTS sci_pages_availability_dag_run ON sci_pages_availability (dag_run);
CREATE INDEX IF NOT EXISTS sci_pages_availability_failed ON sci_pages_availability (failed);

/*
* A tabela doc_deep_checkup guarda o registro do resultado da verificação
  da disponibilidade de URI do padrão do site novo e num âmbito mais profundo,
  ou seja, é verificada todas as URI do documento no padrão do site novo
  (HTML/PDF vs IDIOMAS) e de suas manifestações e seus ativos digitais da forma
  como registradas no _object store_.
  Além disso, quando o formato é o HTML, verificar quais os URI dos ativos
  digitais e dos documentos no padrão do site novo (HTML/PDF vs IDIOMAS)
  estão presentes ou ausentes no HTML. 
  Para cada documento verificado, é criada uma nova entrada, mesmo que haja
  reexecução da DAG
*/
CREATE TABLE IF NOT EXISTS doc_deep_checkup (
  id SERIAL PRIMARY KEY,
  dag_run varchar(255),                             -- Identificador de execução da dag `check_website`
  input_file_name varchar(255) NULL,                -- Nome do arquivo de entrada: csv com PIDs v2 ou uri_list
  pid_v3 varchar(23),                               -- scielo-pid-v3 presente no xml
  pid_v2_journal varchar(9),                        -- ISSN ID do periódico
  pid_v2_issue varchar(17),                         -- PID do fascículo
  pid_v2_doc varchar(23),                           -- PID do documento
  previous_pid_v2_doc varchar(23) NULL,             -- previous PID do documento
  status varchar(8),                                -- "complete" or "partial" or "missing"
  detail json,                                      -- Detalhes da verificação profunda
  created_at timestamptz default now()
);

CREATE INDEX IF NOT EXISTS doc_deep_checkup_pid_v3 ON doc_deep_checkup (pid_v3);
CREATE INDEX IF NOT EXISTS doc_deep_checkup_dag_run ON doc_deep_checkup (dag_run);
CREATE INDEX IF NOT EXISTS doc_deep_checkup_pid_v2_journal ON doc_deep_checkup (pid_v2_journal);
CREATE INDEX IF NOT EXISTS doc_deep_checkup_pid_v2_issue ON doc_deep_checkup (pid_v2_issue);
CREATE INDEX IF NOT EXISTS doc_deep_checkup_pid_v2_doc ON doc_deep_checkup (pid_v2_doc);
CREATE INDEX IF NOT EXISTS doc_deep_checkup_previous_pid_v2_doc ON doc_deep_checkup (previous_pid_v2_doc);
CREATE INDEX IF NOT EXISTS doc_deep_checkup_status ON doc_deep_checkup (status);


/* Exemplo de JSON para doc_deep_checkup
ver tests/fixtures/BrT6FWNFFR3KBKHZVPN8Y9N.json
*/