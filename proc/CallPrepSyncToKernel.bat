rem Disponibiliza pacotes SPS resultantes do XML Converter + scilista + log
rem para o SciELO Publishing Framework 

PREP_LOG=log/PrepSyncToKernel-$(date "+%Y-%m-%d").log
PrepSyncToKernel.bat > $PREP_LOG

if [ -f SyncToKernel.ini ];
then
    . SyncToKernel.ini
fi

if [ "" != "${XC_KERNEL_GATE}" ] && [ -e ${XC_KERNEL_GATE} ];
then
    cp $PREP_LOG ${XC_KERNEL_GATE}
fi
