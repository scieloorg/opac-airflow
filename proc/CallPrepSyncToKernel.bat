# Disponibiliza pacotes SPS resultantes do XML Converter + scilista + log
# para o SciELO Publishing Framework 

PREP_LOG=log/PrepSyncToKernel-$1.log
./PrepSyncToKernel.bat $1 > $PREP_LOG

if [ -f SyncToKernel.ini ];
then
    . ./SyncToKernel.ini
fi

if [ "" != "${XC_KERNEL_GATE}" ] && [ -e ${XC_KERNEL_GATE} ];
then
    cp $PREP_LOG ${XC_KERNEL_GATE}
fi
