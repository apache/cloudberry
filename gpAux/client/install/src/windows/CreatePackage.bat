set GPDB_INSTALL_PATH=%1
set VERSION=%2
echo %VERSION% > %GPDB_INSTALL_PATH%\VERSION
copy ..\..\..\..\..\NOTICE %GPDB_INSTALL_PATH%
copy ..\..\..\..\..\LICENSE %GPDB_INSTALL_PATH%
copy ..\..\..\scripts\greengage_clients_path.bat %GPDB_INSTALL_PATH%
mkdir %GPDB_INSTALL_PATH%\lib\python\yaml
copy ..\..\..\..\..\gpMgmt\bin\gpload.py %GPDB_INSTALL_PATH%\bin
mkdir %GPDB_INSTALL_PATH%\bin\gppylib
type nul > %GPDB_INSTALL_PATH%\bin\gppylib\__init__.py
copy ..\..\..\..\..\gpMgmt\bin\gppylib\gpversion.py %GPDB_INSTALL_PATH%\bin\gppylib\
perl -pi.bak -e "s,\$Revision\$,%VERSION%," %GPDB_INSTALL_PATH%\bin\gpload.py
copy ..\..\..\..\..\gpMgmt\bin\gpload.bat %GPDB_INSTALL_PATH%\bin
for %%f in (..\..\..\..\..\gpMgmt\bin\pythonSrc\ext\PyYAML-*.tar.gz) do tar -xf %%f
for /D %%d in (PyYAML-*) do copy %%d\lib\yaml\* %GPDB_INSTALL_PATH%\lib\python\yaml
perl -p -e "s,__VERSION_PLACEHOLDER__,%VERSION%," greengage-clients.wxs > greengage-clients-%VERSION%.wxs
candle.exe -nologo greengage-clients-%VERSION%.wxs -out greengage-clients-%VERSION%.wixobj -dSRCDIR=%GPDB_INSTALL_PATH% -dVERSION=%VERSION%
light.exe -nologo -sval greengage-clients-%VERSION%.wixobj -out greengage-clients-x86_64.msi