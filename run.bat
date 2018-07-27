:Begin
choice /C:WBC /M:"run：Worker[W] Beat[B] Cancel[C]."
echo "input is :" %errorlevel%
if errorlevel 3 goto End
if errorlevel 2 goto Beat
if errorlevel 1 goto Worker
goto Begin

:Worker
celery -A tasks worker --loglevel=info -c 1 -P eventlet
goto End

:Beat
ehco beat
goto End

:End
echo good bye