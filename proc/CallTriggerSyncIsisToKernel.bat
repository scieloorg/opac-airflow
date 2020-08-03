
# Disponibiliza pacotes SPS resultantes do XML Converter + scilista + log
# para o SciELO Publishing Framework 

TRIGGER_LOG=log/TriggerSyncIsisToKernel-$(date "+%Y-%m-%d").log
./TriggerSyncIsisToKernel.bat > $TRIGGER_LOG

if [ -f SyncToKernel.ini ];
then
    . SyncToKernel.ini
fi

if [ "" != "${XC_KERNEL_GATE}" ] && [ -e ${XC_KERNEL_GATE} ];
then
    cp $TRIGGER_LOG ${XC_KERNEL_GATE}
fi
