echo "run celery worker"
cd /home/mushrooman/wspy/data_integration_celery
source venv/bin/activate
celery -A tasks worker --loglevel=info -c 1 -P eventlet

