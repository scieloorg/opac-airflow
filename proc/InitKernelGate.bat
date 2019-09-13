rem Inicia o Kernel Gate
rem Verifica e Instala o Curl
rem Faz uma requisição HTTP para o opac-airflow


echo "Inicializando InitKernelGate: Sincronizacao com o Kernel..."

export DAGID='kernel-gate'
export AIRFLOW_HOST=http://0.0.0.0:8080
export AIRFLOW_API=$AIRFLOW_HOST/api/experimental/dags/$DAGID/dag_runs

echo "Executando InitKernelGate..."

if [ ! -x "$(command -v curl)" ]
then
    echo '"curl" e uma dependencia do comando InitKernelGate'
    echo
    echo Não foi encontrada curl instalado. Instale curl e tente novamente.
    echo
    echo Tecle CONTROL-C para sair ou ENTER para continuar...
    while [ true ] ; do
        read -t 10 -n 1
        if [ $? = 0 ] ; then
            exit ;
        else
            echo "Aguardando tecla para sair"
        fi
    done
else
    echo Acessando a URL $AIRFLOW_API para iniciar sincronizacao com o Kernel
    curl -XPOST "$AIRFLOW_API" -H 'Cache-Control: no-cache' -H 'Content-Type: application/json' -d '{}'
fi
