echo off

cd D:\WSPych\data_integration_celery
:Begin
choice /C:AWBCTJ /M:"run: Active Env[A] Worker[W] Beat[B] Cancel[C] Tushare Local[T] Tushare Local First[F] Join Quant Local[J]"
echo "input is :" %errorlevel%
if errorlevel 7 goto JoinQuantLocal
if errorlevel 6 goto TushareLocalFirst
if errorlevel 5 goto TushareLocal
if errorlevel 4 goto End
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

:TushareLocalFirst
.\venv\Scripts\Python.exe -m tasks.tushare.app_tasks first
goto End

:TushareLocal
.\venv\Scripts\Python.exe -m tasks.tushare.app_tasks
goto End

:JoinQuantLocal
.\venv\Scripts\Python.exe -m tasks.jqdata.app_tasks
goto End

:End

echo on
echo good bye