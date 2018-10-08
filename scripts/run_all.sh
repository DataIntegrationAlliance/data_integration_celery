screen_name="worker"
echo create $screen_name
screen -dmS $screen_name
screen -x -S $screen_name -p 0 -X stuff "cd /home/mushrooman/wspy/run_sh\n"
screen -x -S $screen_name -p 0 -X stuff "./worker.sh\n"

screen_name="beat"
echo create $screen_name
screen -dmS $screen_name
screen -x -S $screen_name -p 0 -X stuff "cd /home/mushrooman/wspy/run_sh\n"
screen -x -S $screen_name -p 0 -X stuff "./beat.sh\n"

screen_name="feeder"
echo create $screen_name
screen -dmS $screen_name
screen -x -S $screen_name -p 0 -X stuff "cd /home/mushrooman/wspy/run_sh\n"
screen -x -S $screen_name -p 0 -X stuff "./feeder.sh\n"

screen_name="trader"
echo create $screen_name
screen -dmS $screen_name
screen -x -S $screen_name -p 0 -X stuff "cd /home/mushrooman/wspy/run_sh\n"
screen -x -S $screen_name -p 0 -X stuff "./trader.sh\n"

screen -ls
