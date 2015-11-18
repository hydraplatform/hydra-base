@ECHO OFF
setlocal
set PYTHONPATH=%~dp0python;%~dp0..\HydraLib\python;
echo %PYTHONPATH%
python server.py
endlocal
