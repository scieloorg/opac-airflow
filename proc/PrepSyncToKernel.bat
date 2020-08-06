# Prepara Sincronizacao com Kernel
# Copia Scilista para diretorio do Escalonador
# Copia Pacotes da lista para diretorio do Escalonador

# SCILISTA_PATH: path do arquivo scilista que sera usado no processamento iniciado pelo GeraPadrao
# XC_SPS_PACKAGES: path do diretório com todos os pacotes gerados pelo XC
# XC_KERNEL_GATE: path do diretório para copia dos pacotes como estao no momento que o processamento do GeraPadrao e iniciado

TODAY_DATE=$(date "+%Y-%m-%d")

echo ""
echo "$TODAY_DATE - Executing $0 from `pwd`"
echo ""

if [ -f SyncToKernel.ini ];
then
    echo "VARIABLES read from file SyncToKernel.ini"
    . SyncToKernel.ini
    echo
    echo SCILISTA_PATH=$SCILISTA_PATH
    echo XC_SPS_PACKAGES=$XC_SPS_PACKAGES
    echo XC_KERNEL_GATE=$XC_KERNEL_GATE
    echo
fi

if [ "" == "${SCILISTA_PATH}" ];
then
    echo "Missing required variable: SCILISTA_PATH"
    ERROR=1
else
    if [ ! -f ${SCILISTA_PATH} ];
    then
        echo "Missing file: ${SCILISTA_PATH}"
        ERROR=1
    fi
fi

if [ "" == "${XC_SPS_PACKAGES}" ];
then
    echo "Missing required variable: XC_SPS_PACKAGES"
    ERROR=1
else
    if [ ! -e ${XC_SPS_PACKAGES} ];
    then
        echo "Missing directory: ${XC_SPS_PACKAGES}. ${XC_SPS_PACKAGES} is not a directory. "
        ERROR=1
    fi
fi

if [ "" == "${XC_KERNEL_GATE}" ];
then
    echo "Missing required variable: XC_KERNEL_GATE"
    ERROR=1
else
    if [ ! -e ${XC_KERNEL_GATE} ];
    then
        echo "Missing directory: ${XC_KERNEL_GATE}. ${XC_KERNEL_GATE} is not a directory. "
        ERROR=1
    fi
fi

if [ "$ERROR" == "1" ];
then
    echo
    echo "SCILISTA_PATH, XC_SPS_PACKAGES e XC_KERNEL_GATE sao obrigatorias para a Syncronizacao com o Kernel."
    echo "Verifique se as tres variaveis estao configuradas."
    echo "A execucao do GeraPadrao seguira sem o Kernel."
    echo
    exit 1
fi


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
echo
echo ===============

SCILISTA_PATH_TMP=/tmp/scilista.lst
cp $SCILISTA_PATH $SCILISTA_PATH_TMP
if [ -e $SCILISTA_PATH_TMP ];
then
    cat $SCILISTA_PATH_TMP | sort -u > $SCILISTA_PATH
fi

if [ -f ${SCILISTA_PATH} ] && [ -e ${XC_SPS_PACKAGES} ] && [ -e ${XC_KERNEL_GATE} ];
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
            PATTERN="${XC_SPS_PACKAGES}/*_${ACRON}_${ISSUE}.zip"
            for PACK_FILE in ${PATTERN};
            do
                echo ${PACK_FILE}
                echo -------------------
                if [ -f "${PACK_FILE}" ];
                then
                    echo "  Moving pack ${PACK_FILE} to ${XC_KERNEL_GATE} ..."
                    echo
                    rsync -qa --inplace --remove-source-files "${PACK_FILE}" ${XC_KERNEL_GATE}
                else
                    if [[ "$ISSUE" == *"ahead"* ]];
                    then
                        echo "  WARNING: Not found ${PACK_FILE} to move"
                        echo "WARNING: Not found ${PACK_FILE} to move" >> $ERRORFILE
                        echo
                    else
                        echo "  ERROR: Not found ${PACK_FILE} to move"
                        echo "ERROR: Not found ${PACK_FILE} to move" >> $ERRORFILE
                        echo
                    fi
                fi
            done
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
    echo "Copiando scilista de $SCILISTA_PATH para a área do Escalonador em ${XC_KERNEL_GATE}/scilista-$TODAY_DATE.lst"
    echo

    cp ${SCILISTA_PATH} "${XC_KERNEL_GATE}/scilista-$TODAY_DATE.lst"

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
    exit 1
fi
