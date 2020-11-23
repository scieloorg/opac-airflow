
COMPOSE_FILE_DEV = docker-compose-dev.yml
COMPOSE_FILE_PROD = docker-compose.yml

help: ## show this help
	@echo 'usage: make [target] ...'
	@echo ''
	@echo 'targets:'
	@egrep '^(.+)\:\ .*##\ (.+)' ${MAKEFILE_LIST} | sed 's/:.*##/#/' | column -t -c 2 -s '#'

############################################
## atalhos docker-compose desenvolvimento ##
############################################

dev_compose_build:  ## Build app using $(COMPOSE_FILE_DEV)
	@docker-compose -f $(COMPOSE_FILE_DEV) build

dev_compose_up:  ## Start app using $(COMPOSE_FILE_DEV)
	@docker-compose -f $(COMPOSE_FILE_DEV) up -d

dev_compose_logs: ## See all app logs using $(COMPOSE_FILE_DEV)
	@docker-compose -f $(COMPOSE_FILE_DEV) logs -f

dev_compose_stop:  ## Stop all app using $(COMPOSE_FILE_DEV)
	@docker-compose -f $(COMPOSE_FILE_DEV) stop

dev_compose_ps:  ## See all containers using $(COMPOSE_FILE_DEV)
	@docker-compose -f $(COMPOSE_FILE_DEV) ps

dev_compose_rm:  ## Remove all containers using $(COMPOSE_FILE_DEV)
	@docker-compose -f $(COMPOSE_FILE_DEV) rm -f

dev_compose_bash:  ## Open python terminal from opac-airflow $(COMPOSE_FILE_DEV)
	@docker-compose -f $(COMPOSE_FILE_DEV) run --rm opac-airflow bash

dev_compose_add_opac_conn:  ## Add default opac_conn
	@docker-compose -f $(COMPOSE_FILE_DEV) run --rm opac-airflow airflow connections \
	-a --conn_type=Mongo \
	--conn_id=opac_conn \
	--conn_host=localhost \
	--conn_schema=opac \
	--conn_port=27017 \
	--conn_extra='{"authentication_source": "admin"}'

dev_compose_add_kernel_conn:  ## Add default kernel_conn
	@docker-compose -f $(COMPOSE_FILE_DEV) run --rm opac-airflow airflow connections \
	    -a --conn_type=HTTP \
	    --conn_id=kernel_conn \
	    --conn_host=http://0.0.0.0 \
	    --conn_port=6543

dev_compose_add_postgres_report_conn:
	@docker-compose -f $(COMPOSE_FILE_DEV) run --rm opac-airflow airflow connections \
	-a --conn_type=Postgres \
	--conn_id=postgres_report_connection

dev_compose_rm_db:  ## Remove database
	@docker-compose -f $(COMPOSE_FILE_DEV) run --rm opac-airflow rm airflow.db

dev_compose_initdb:  ## Initialize airflow database
	@docker-compose -f $(COMPOSE_FILE_DEV) run --rm opac-airflow airflow initdb

############################################
## atalhos docker-compose produção        ##
############################################

compose_build:  ## Build app using $(COMPOSE_FILE_PROD)
	@docker-compose -f $(COMPOSE_FILE_PROD) build

compose_up:  ## Start app using $(COMPOSE_FILE_PROD)
	@docker-compose -f $(COMPOSE_FILE_PROD) up -d

compose_logs: ## See all app logs using $(COMPOSE_FILE_PROD)
	@docker-compose -f $(COMPOSE_FILE_PROD) logs -f

compose_stop:  ## Stop all app using $(COMPOSE_FILE_PROD)
	@docker-compose -f $(COMPOSE_FILE_PROD) stop

compose_ps:  ## See all containers using $(COMPOSE_FILE_PROD)
	@docker-compose -f $(COMPOSE_FILE_PROD) ps

compose_rm:  ## Remove all containers using $(COMPOSE_FILE_PROD)
	@docker-compose -f $(COMPOSE_FILE_PROD) rm -f

compose_bash:  ## Open python terminal from portal $(COMPOSE_FILE_PROD)
	@docker-compose -f $(COMPOSE_FILE_PROD) run --rm opac-airflow bash

compose_add_opac_conn:  ## Add default opac_conn
	@docker-compose -f $(COMPOSE_FILE_PROD) run --rm opac-airflow airflow connections \
	-a --conn_type=Mongo \
	--conn_id=opac_conn \
	--conn_host=localhost \
	--conn_schema=opac \
	--conn_port=27017 \
	--conn_extra='{"authentication_source": "admin"}'

compose_add_postgres_report_conn:
	@docker-compose -f $(COMPOSE_FILE_PROD) run --rm opac-airflow airflow connections \
	-a --conn_type=Postgres \
	--conn_id=postgres_report_connection

compose_add_kernel_conn:  ## Add default kernel_conn
	@docker-compose -f $(COMPOSE_FILE_PROD) run --rm opac-airflow airflow connections \
	    -a --conn_type=HTTP \
	    --conn_id=kernel_conn \
	    --conn_host=http://0.0.0.0 \
	    --conn_port=6543

compose_rm_db:  ## Remove database
	@docker-compose -f $(COMPOSE_FILE_PROD) run --rm opac-airflow rm airflow.db

compose_initdb:  ## Initialize airflow database
	@docker-compose -f $(COMPOSE_FILE_PROD) run --rm opac-airflow airflow initdb

#####################################################
## atalhos docker-compose build e testes no travis ##
#####################################################

travis_compose_build:
	@echo "[Travis Build] compose file: " $(COMPOSE_FILE_PROD)
	@docker-compose -f $(COMPOSE_FILE_PROD) build

travis_compose_up:
	@docker-compose -f $(COMPOSE_FILE_PROD) up -d

travis_compose_make_test:
	@docker-compose -f $(COMPOSE_FILE_PROD) exec opac-airflow bash -c "export AIRFLOW__CORE__SQL_ALCHEMY_CONN=sqlite:////usr/local/airflow/airflow.db && airflow initdb && python -m unittest -v"