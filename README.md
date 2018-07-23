# data_integration_celery
通过celery定期执行更相关任务，将万得wind，同花顺ifind，东方财富choice等数据终端的数据进行整合，清洗，一致化，供其他系统数据分析使用

### 环境依赖
+ windows
+ rabbitmq

为了支持独立运行在windows环境下，celery 的 broker 选择 rabbitmq 而非 redis（仅支持linux）

#### rabbitmq 系统配置
创建用户，host，及访问权限
```commandline
rabbitmqctl add_user mg ****
rabbitmqctl set_user_tags mg broker_backend

rabbitmqctl add_vhost celery_tasks
rabbitmqctl set_permissions -p celery_tasks mg ".*" ".*" ".*"

rabbitmqctl add_vhost backend
rabbitmqctl set_permissions -p backend mg ".*" ".*" ".*"
```

### 启动 celery
#### 启动 worker
```commandline
celery -A tasks worker --loglevel=info -c 1 -P eventlet
```
其中 -P 命令只要是为了在win10 下可以正常运行 [issue](https://github.com/celery/celery/issues/4081) 其他环境下可以去除


