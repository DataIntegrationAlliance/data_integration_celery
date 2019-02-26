echo off

cd D:\WSPych\data_integration_celery
:Begin
choice /C:AWBSC /M:"run: Active Evn[A] Worker[W] Beat[B] SingleWorker[S] Cancel[C]."
echo "input is :" %errorlevel%
if errorlevel 5 goto End
if errorlevel 4 goto SingleWorker
if errorlevel 3 goto Beat
if errorlevel 2 goto Worker
if errorlevel 1 goto Active
goto Begin

:Active
D:\WSPych\data_integration_celery\venv\Scripts\activate.bat
goto End

:Worker
celery -A tasks worker --loglevel=info -c 1 -P eventlet
goto End

:Beat
celery beat -A tasks
goto End

:SingleWorker
.\venv\Scripts\Python.exe -m tasks.tushare.__init__
goto End

:End

echo on
echo good bye