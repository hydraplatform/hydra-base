set PYTHONHOME=%PUBLIC%\Documents\Hydra\Python27
set PATH=%PUBLIC%\Documents\Hydra\Python27\DLLS
set PYTHONPATH=%HYDRA_SRC%\HydraLib\python\;%HYDRA_SRC%\HydraServer\python
%PYTHONHOME%\hydra_python.exe %HYDRA_SRC%\HydraServer\server.py
pause
