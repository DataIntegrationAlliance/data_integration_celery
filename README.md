# data_integration_celery 
[![Build Status](https://travis-ci.org/DataIntegrationAlliance/data_integration_celery.svg?branch=master)](https://travis-ci.org/DataIntegrationAlliance/data_integration_celery) [![GitHub issues](https://img.shields.io/github/issues/DataIntegrationAlliance/data_integration_celery.svg)](https://github.com/DataIntegrationAlliance/data_integration_celery/issues) [![GitHub forks](https://img.shields.io/github/forks/DataIntegrationAlliance/data_integration_celery.svg)](https://github.com/DataIntegrationAlliance/data_integration_celery/network) [![GitHub stars](https://img.shields.io/github/stars/DataIntegrationAlliance/data_integration_celery.svg)](https://github.com/DataIntegrationAlliance/data_integration_celery/stargazers) [![GitHub license](https://img.shields.io/github/license/DataIntegrationAlliance/data_integration_celery.svg)](https://github.com/DataIntegrationAlliance/data_integration_celery/blob/master/LICENSE) [![Twitter](https://img.shields.io/twitter/url/https/github.com/DataIntegrationAlliance/data_integration_celery.svg?style=social)](https://twitter.com/intent/tweet?text=Wow:&url=https%3A%2F%2Fgithub.com%2FDataIntegrationAlliance%2Fdata_integration_celery) [![HitCount](http://hits.dwyl.io/DataIntegrationAlliance/https://github.com/DataIntegrationAlliance/data_integration_celery.svg)](http://hits.dwyl.io/DataIntegrationAlliance/https://github.com/DataIntegrationAlliance/data_integration_celery)

通过celery定期执行更相关任务，将万得wind，同花顺ifind，东方财富choice等数据终端的数据进行整合，清洗，一致化，供其他系统数据分析使用

### 环境依赖及安装配置
+ windows
+ rabbitmq

为了支持独立运行在windows环境下，celery 的 broker 选择 rabbitmq 而非 redis（仅支持linux）

#### RabbitMQ 系统配置
创建用户，host，及访问权限
```commandline
rabbitmqctl add_user mg ****
rabbitmqctl set_user_tags mg broker_backend

rabbitmqctl add_vhost celery_tasks
rabbitmqctl set_permissions -p celery_tasks mg ".*" ".*" ".*"

rabbitmqctl add_vhost backend
rabbitmqctl set_permissions -p backend mg ".*" ".*" ".*"
```
#### 启动web端
rabbitmq-plugins enable rabbitmq_management

[RabbitMQ 管理界面](http://localhost:15672/#/connections)

### 启动 celery
#### 启动 worker
```commandline
celery -A tasks worker --loglevel=info -c 1 -P eventlet
```
其中 -P 命令只要是为了在win10 下可以正常运行 [issue](https://github.com/celery/celery/issues/4081) 其他环境下可以去除


