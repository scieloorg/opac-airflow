export PATH=$PATH:.
export TABS=tabs
rem Este arquivo eh uma chamada para: 
rem Preparação da sincronização dos dados para o Kernel
rem GeraScielo.bat com parametros STANDARD


clear
echo === ATENCAO ===
echo 
echo Este arquivo executara os seguintes comandos
echo nohup CallPrepSyncToKernel.bat > /tmp/CallPrepSyncToKernel.out&
echo GeraScielo.bat .. /scielo/web log/GeraPadrao.log adiciona
echo 


nohup CallPrepSyncToKernel.bat > /tmp/CallPrepSyncToKernel.out&

GeraScielo.bat .. .. log/GeraPadrao.log adiciona

nohup TriggerSyncIsisToKernel.bat > /tmp/CallTriggerSyncIsisToKernel.out&