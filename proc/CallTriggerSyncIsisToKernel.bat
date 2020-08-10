
# Disponibiliza pacotes SPS resultantes do XML Converter + scilista + log
# para o SciELO Publishing Framework 

GERAPADRAO_ID=$1
TRIGGER_LOG=log/TriggerSyncIsisToKernel-${GERAPADRAO_ID}.log
./TriggerSyncIsisToKernel.bat ${GERAPADRAO_ID} > $TRIGGER_LOG

if [ -f SyncToKernel.ini ];
then
    . SyncToKernel.ini
fi

if [ "" != "${XC_KERNEL_GATE}" ] && [ -e ${XC_KERNEL_GATE} ];
then
    cp $TRIGGER_LOG ${XC_KERNEL_GATE}
fi
