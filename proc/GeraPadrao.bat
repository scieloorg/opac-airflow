export PATH=$PATH:.
export TABS=tabs
rem Este arquivo ?uma chamada para o 
rem GeraScielo.bat com par?etros STANDARD

clear
echo === ATENCAO ===
echo 
echo Este arquivo executara os seguintes comandos
echo PrepSyncToKernel.bat
echo GeraScielo.bat .. /scielo/web log/GeraPadrao.log adiciona
echo 

PREP_LOG=log/PrepSyncToKernel.log
PrepSyncToKernel.bat > $PREP_LOG

if [ -f PrepSyncToKernel.ini ];
then
    . PrepSyncToKernel.ini
fi

if [ "" != "${XC_KERNEL_GATE}" ] && [ -e ${XC_KERNEL_GATE} ];
then
    cp $PREP_LOG ${XC_KERNEL_GATE}
fi

GeraScielo.bat .. .. log/GeraPadrao.log adiciona
