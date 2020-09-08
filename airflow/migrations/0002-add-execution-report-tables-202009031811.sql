/*
  A tabela pid_v2 guarda o registro do PID v2 e o respectivo PID v3
  Para cada PID v2, é criada uma entrada única,
  exceto se o valor de pid_v3 divergir
*/ 
CREATE TABLE IF NOT EXISTS pid_v2 (
  id SERIAL PRIMARY KEY,
  pid_v2_journal varchar(9),                    -- ISSN ID do periódico
  pid_v2_issue varchar(17),                     -- PID do fascículo
  pid_v2_doc varchar(23),                       -- PID do documento
  pid_v3 varchar(23) NULL,                      -- PID v3 do documento
  pid_previous varchar(23) NULL,                -- previous PID v2 do documento
  created_at timestamptz DEFAULT now()
  updated_at timestamptz DEFAULT now()
);

CREATE INDEX IF NOT EXISTS pid_v2_pid_v2_journal ON pid_v2 (pid_v2_journal);
CREATE INDEX IF NOT EXISTS pid_v2_pid_v2_issue ON pid_v2 (pid_v2_issue);
CREATE INDEX IF NOT EXISTS pid_v2_pid_v2_doc ON pid_v2 (pid_v2_doc);
CREATE INDEX IF NOT EXISTS pid_v3 ON pid_v2 (pid_v3);

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
  input_file_name varchar(255),                     -- Nome do arquivo de entrada: csv com PIDs v2 ou uri_list
  pid_v3 varchar(23) NULL,                          -- scielo-pid-v3 presente no xml
  failed bool DEFAULT false,                        -- Falha == true
  detail json,                                      -- Detalhes da verificação profunda
  created_at timestamptz default now()
);

CREATE INDEX IF NOT EXISTS doc_deep_checkup_pid_v3 ON doc_deep_checkup (pid_v3);
CREATE INDEX IF NOT EXISTS doc_deep_checkup_dag_run ON doc_deep_checkup (dag_run);


/* Exemplo de JSON para doc_deep_checkup

{
   "summary": {
      "total doc webpages": 2,
      "total doc renditions": 1,
      "total doc assets": 3,
      "total missing components": 2,
      "total expected components": 4,
      "total unavailable doc webpages": 0,
      "total unavailable renditions": 0,
      "total unavailable assets": 0
   },
   "detail": {
      "doc webpages availability": [
         {
            "lang": "pt",
            "format": "html",
            "pid_v2": "S1413-41522020005004201",
            "acron": "esa",
            "doc_id_for_human": "1809-4457-esa-ahead-S1413-41522020182506",
            "doc_id": "BrT6FWNFFR3KBKHZVPN8Y9N",
            "uri": "https://www.scielo.br/j/esa/a/BrT6FWNFFR3KBKHZVPN8Y9N?format=html&lang=pt",
            "available": true,
            "status code": 200,
            "start time": "start timestamp",
            "end time": "end timestamp",
            "components": [
               {
                  "type": "asset",
                  "id": "1809-4457-esa-s1413-41522020182506-gf1",
                  "present_in_html": [
                     "https://minio.scielo.br/documentstore/1809-4457/BrT6FWNFFR3KBKHZVPN8Y9N/8972aaa0916382b6f2d51a6d22732bb083851913.png"
                  ],
                  "absent_in_html": [
                     "https://minio.scielo.br/documentstore/1809-4457/BrT6FWNFFR3KBKHZVPN8Y9N/3c30f9fec6947d47f404043fe08aaca8bc51b1fb.jpg"
                  ]
               },
               {
                  "type": "asset",
                  "id": "1809-4457-esa-s1413-41522020182506-gf2",
                  "present_in_html": [],
                  "absent_in_html": [
                     "https://minio.scielo.br/documentstore/1809-4457/BrT6FWNFFR3KBKHZVPN8Y9N/36080074121a60c8e28fa1b28876e1adad4fe5d7.png",
                     "https://minio.scielo.br/documentstore/1809-4457/BrT6FWNFFR3KBKHZVPN8Y9N/b5b4bb9bc267794ececde428a33f5af705b0b1a6.jpg"
                  ]
               },
               {
                  "type": "asset",
                  "id": "1809-4457-esa-s1413-41522020182506-gf3",
                  "present_in_html": [],
                  "absent_in_html": [
                     "https://minio.scielo.br/documentstore/1809-4457/BrT6FWNFFR3KBKHZVPN8Y9N/73a98051b6cf623aeb1146017ceb0b947df75ec8.png",
                     "https://minio.scielo.br/documentstore/1809-4457/BrT6FWNFFR3KBKHZVPN8Y9N/df14e57dc001993fd7f3fbcefa642e40e6964224.jpg"
                  ]
               },
               {
                  "type": "pdf",
                  "id": "pt",
                  "present_in_html": [
                     "/j/esa/a/BrT6FWNFFR3KBKHZVPN8Y9N/?format=pdf&lang=pt"
                  ]
               }
            ],
            "expected components quantity": 4,
            "missing components quantity": 2,
            "existing_uri_items_in_html": [
               "/about/",
               "/j/esa/",
               "/j/esa/a/BrT6FWNFFR3KBKHZVPN8Y9N/?format=pdf&lang=pt",
               "/j/esa/a/BrT6FWNFFR3KBKHZVPN8Y9N/?lang=pt",
               "/j/esa/a/S1413-41522020005004201/?format=pdf&lang=pt",
               "/j/esa/a/TWyHMQBS4H6tyrXPZhcWxps/",
               "/j/esa/a/nhg9DgZSsvnhXjq7qCN7cvc/",
               "/j/esa/i/9999.nahead/",
               "/journal/esa/about/#about",
               "/journal/esa/about/#contact",
               "/journal/esa/about/#editors",
               "/journal/esa/about/#instructions",
               "/journal/esa/feed/",
               "/journals/alpha?status=current",
               "/journals/thematic?status=current",
               "/media/images/esa_glogo.gif",
               "/set_locale/es/",
               "/set_locale/pt_BR/",
               "/static/img/oa_logo_32.png",
               "/static/js/scielo-article-min.js",
               "/static/js/scielo-bundle-min.js",
               "/static/js/scienceopen.js",
               "https://minio.scielo.br/documentstore/1809-4457/BrT6FWNFFR3KBKHZVPN8Y9N/8972aaa0916382b6f2d51a6d22732bb083851913.png"
            ]
         },
         {
            "lang": "pt",
            "format": "pdf",
            "pid_v2": "S1413-41522020005004201",
            "acron": "esa",
            "doc_id_for_human": "1809-4457-esa-ahead-S1413-41522020182506",
            "doc_id": "BrT6FWNFFR3KBKHZVPN8Y9N",
            "uri": "https://www.scielo.br/j/esa/a/BrT6FWNFFR3KBKHZVPN8Y9N?format=pdf&lang=pt",
            "available": true,
            "status code": 200,
            "start time": "start timestamp",
            "end time": "end timestamp"
         }
      ],
      "doc renditions availability": [
         {
            "lang": "pt",
            "uri": "https://minio.scielo.br/documentstore/1809-4457/BrT6FWNFFR3KBKHZVPN8Y9N/409acdeb8f632022d41b3d94a3f00a837867937c.pdf",
            "available": true,
            "status code": 200,
            "start time": "start timestamp",
            "end time": "end timestamp"
         }
      ],
      "doc assets availability": [
         {
            "asset_id": "1809-4457-esa-s1413-41522020182506-gf1.png",
            "uri": "https://minio.scielo.br/documentstore/1809-4457/BrT6FWNFFR3KBKHZVPN8Y9N/8972aaa0916382b6f2d51a6d22732bb083851913.png",
            "available": true,
            "status code": 200,
            "start time": "start timestamp",
            "end time": "end timestamp"
         },
         {
            "asset_id": "1809-4457-esa-s1413-41522020182506-gf1.thumbnail.jpg",
            "uri": "https://minio.scielo.br/documentstore/1809-4457/BrT6FWNFFR3KBKHZVPN8Y9N/3c30f9fec6947d47f404043fe08aaca8bc51b1fb.jpg",
            "available": true,
            "status code": 200,
            "start time": "start timestamp",
            "end time": "end timestamp"
         },
         {
            "asset_id": "1809-4457-esa-s1413-41522020182506-gf2.png",
            "uri": "https://minio.scielo.br/documentstore/1809-4457/BrT6FWNFFR3KBKHZVPN8Y9N/36080074121a60c8e28fa1b28876e1adad4fe5d7.png",
            "available": true,
            "status code": 200,
            "start time": "start timestamp",
            "end time": "end timestamp"
         },
         {
            "asset_id": "1809-4457-esa-s1413-41522020182506-gf2.thumbnail.jpg",
            "uri": "https://minio.scielo.br/documentstore/1809-4457/BrT6FWNFFR3KBKHZVPN8Y9N/b5b4bb9bc267794ececde428a33f5af705b0b1a6.jpg",
            "available": true,
            "status code": 200,
            "start time": "start timestamp",
            "end time": "end timestamp"
         },
         {
            "asset_id": "1809-4457-esa-s1413-41522020182506-gf3.png",
            "uri": "https://minio.scielo.br/documentstore/1809-4457/BrT6FWNFFR3KBKHZVPN8Y9N/73a98051b6cf623aeb1146017ceb0b947df75ec8.png",
            "available": true,
            "status code": 200,
            "start time": "start timestamp",
            "end time": "end timestamp"
         },
         {
            "asset_id": "1809-4457-esa-s1413-41522020182506-gf3.thumbnail.jpg",
            "uri": "https://minio.scielo.br/documentstore/1809-4457/BrT6FWNFFR3KBKHZVPN8Y9N/df14e57dc001993fd7f3fbcefa642e40e6964224.jpg",
            "available": true,
            "status code": 200,
            "start time": "start timestamp",
            "end time": "end timestamp"
         }
      ]
   }
}

*/