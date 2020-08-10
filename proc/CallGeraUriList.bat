# CallGeraUriList.bat gera lista de uri do site para serem verificadas
# deve ser executado após o GeraScielo.bat
# ou após as bases-work/acron/acron estarem atualizadas
# A lista fica disponibilizada em XC_KERNEL_GATE

if [ -f SyncToKernel.ini ];
then
    echo "VARIABLES read from file SyncToKernel.ini"
    . ./SyncToKernel.ini
    echo
    echo SCILISTA_PATH=$SCILISTA_PATH
    echo XC_SPS_PACKAGES=$XC_SPS_PACKAGES
    echo XC_KERNEL_GATE=$XC_KERNEL_GATE
    echo CISIS_DIR=$CISIS_DIR
    echo
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
if [ "" == "${CISIS_DIR}" ];
then
    echo "Missing required variable: CISIS_DIR"
    ERROR=1
else
    if [ ! -f ${CISIS_DIR} ];
    then
        echo "Missing file: ${CISIS_DIR} "
        ERROR=1
    fi
fi
if [ "$ERROR" == "1" ];
then
    exit 1
fi

# Garante que o valor de `TMP_SCRIPT` seja sempre novo
# evitando de executar um script antigo

TMP_SCRIPT=/tmp/GeraUriList_$(date "+%Y-%m-%d-%H%M%s").bat

$CISIS_DIR/mx "seq=scilista.lst " lw=9000 "pft=if p(v1) and p(v2) and a(v3) then './GeraUriList.bat ',v1,' ',v2/ fi" now >${TMP_SCRIPT}

chmod 755 ${TMP_SCRIPT}

URI_LIST=${XC_KERNEL_GATE}/uri_list_$(date "+%Y-%m-%d").lst

${TMP_SCRIPT} > ${URI_LIST}

rm ${TMP_SCRIPT}

