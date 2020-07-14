# Prepara Sincronizacao com Kernel
# Copia Scilista para diretorio do Escalonador
# Copia Pacotes da lista para diretorio do Escalonador

# SCILISTA_PATH: path do arquivo scilista que sera usado no processamento iniciado pelo GeraPadrao
# XC_SPS_PACKAGES: path do diretório com todos os pacotes gerados pelo XC
# XC_KERNEL_GATE: path do diretório para copia dos pacotes como estao no momento que o processamento do GeraPadrao e iniciado


ERRORFILE=/tmp/PrepSyncToKernel.err
echo > $ERRORFILE

echo ===============
echo
echo "Prepara Sincronizacao com o Kernel..."
echo "Copiando pacotes SPS para a área do Escalonador"
echo
echo SCILISTA_PATH=$SCILISTA_PATH
echo XC_SPS_PACKAGES=$XC_SPS_PACKAGES
echo XC_KERNEL_GATE=$XC_KERNEL_GATE
echo ERRORFILE=$ERRORFILE
echo
echo ===============

SCILISTA_PATH_TMP=/tmp/scilista.lst
cp $SCILISTA_PATH $SCILISTA_PATH_TMP
if [ -e $SCILISTA_PATH_TMP ];
then
    cat $SCILISTA_PATH_TMP | sort -u > $SCILISTA_PATH
fi

if [ ! -z ${SCILISTA_PATH+x} ] && [ ! -z ${XC_SPS_PACKAGES+x} ] && [ ! -z ${XC_KERNEL_GATE+x} ];
then
    while read LINE; do
        ACRON="$(echo $LINE | cut -f1 -d ' ')"
        ISSUE="$(echo $LINE | cut -f2 -d ' ')"
        DEL_COMMAND="$(echo $LINE | cut -f3 -d ' ')"
        echo
        echo "ACRON: $ACRON | ISSUE: $ISSUE"
        echo

        if [[ $(tr '[:upper:]' '[:lower:]' <<< "$DEL_COMMAND") = del ]];
        then
            echo "  Package to delete: ${ACRON}_${ISSUE}"
            echo
        else
            PACK_NAME="${XC_SPS_PACKAGES}/*${ACRON}_${ISSUE}.zip"
            if [ -e ${PACK_NAME} ];
            then
                echo "  Moving pack ${PACK_NAME} to ${XC_KERNEL_GATE} ..."
                echo
                mv ${PACK_NAME} ${XC_KERNEL_GATE}
            else
                if [[ "$ISSUE" == *"ahead"* ]];
                then
                    echo "  WARNING: Not found ${PACK_NAME} to move"
                    echo "WARNING: Not found ${PACK_NAME} to move" >> $ERRORFILE
                    echo
                else
                    echo "  ERROR: Not found ${PACK_NAME} to move"
                    echo "ERROR: Not found ${PACK_NAME} to move" >> $ERRORFILE
                    echo
                fi
            fi
        fi
    done < $SCILISTA_PATH

    echo "--------------------------------------------------------"
    echo "Number of items: "
    echo "`cat ${SCILISTA_PATH_TMP} | wc -l` in ${SCILISTA_PATH} (original)"
    echo "`cat ${SCILISTA_PATH} | wc -l` in ${SCILISTA_PATH} (no repetition)"
    echo "`ls ${XC_SPS_PACKAGES} | wc -l` in ${XC_SPS_PACKAGES}"
    echo "`ls ${XC_KERNEL_GATE} | wc -l` in ${XC_KERNEL_GATE}"
    echo "--------------------------------------------------------"
    grep WARNING $ERRORFILE
    grep ERROR $ERRORFILE
    echo "--------------------------------------------------------"
 
    echo
    echo "Copiando scilista de $SCILISTA_PATH para a área do Escalonador em ${XC_KERNEL_GATE}"
    echo

    cp ${SCILISTA_PATH} ${XC_KERNEL_GATE}

    echo
    echo "SPS Packages and Scilista copied successfully!"
    echo
else
    echo
    echo "SCILISTA_PATH, XC_SPS_PACKAGES e XC_KERNEL_GATE sao obrigatorias para a Syncronizacao com o Kernel."
    echo "Verifique se as tres variaveis estao configuradas."
    echo "A execucao do proc seguira sem o Kernel."
    echo
    echo ===============
fi
