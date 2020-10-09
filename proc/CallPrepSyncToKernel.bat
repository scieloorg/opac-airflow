# Disponibiliza pacotes SPS resultantes do XML Converter + scilista + log
# para o SciELO Publishing Framework 

ID_PROC=${1}
PREP_LOG=log/PrepSyncToKernel-${ID_PROC}.log
./PrepSyncToKernel.bat ${ID_PROC} > $PREP_LOG

if [ -f SyncToKernel.ini ];
then
    . ./SyncToKernel.ini
fi

if [ "" != "${XC_KERNEL_GATE}" ] && [ -e ${XC_KERNEL_GATE} ];
then
    cp $PREP_LOG ${XC_KERNEL_GATE}
fi

if [ "" != "${EMAIL_TO}" ];
then
    mail -s "`cat /tmp/subject-${ID_PROC}.txt`" ${EMAIL_TO} < ${PREP_LOG}
fi
