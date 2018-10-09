echo "run celery beat"
cd /home/mushrooman/wspy/data_integration_celery
source venv/bin/activate
celery beat -A tasks
