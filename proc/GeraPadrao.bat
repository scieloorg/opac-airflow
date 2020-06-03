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
echo Tecle CONTROL-C para sair ou ENTER para continuar...

PrepSyncToKernel.bat
GeraScielo.bat .. .. log/GeraPadrao.log adiciona
