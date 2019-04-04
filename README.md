# data_integration_celery 
[![Build Status](https://travis-ci.org/DataIntegrationAlliance/data_integration_celery.svg?branch=master)](https://travis-ci.org/DataIntegrationAlliance/data_integration_celery)
[![GitHub issues](https://img.shields.io/github/issues/DataIntegrationAlliance/data_integration_celery.svg)](https://github.com/DataIntegrationAlliance/data_integration_celery/issues)
[![GitHub forks](https://img.shields.io/github/forks/DataIntegrationAlliance/data_integration_celery.svg)](https://github.com/DataIntegrationAlliance/data_integration_celery/network)
[![GitHub stars](https://img.shields.io/github/stars/DataIntegrationAlliance/data_integration_celery.svg)](https://github.com/DataIntegrationAlliance/data_integration_celery/stargazers) 
[![GitHub license](https://img.shields.io/github/license/DataIntegrationAlliance/data_integration_celery.svg)](https://github.com/DataIntegrationAlliance/data_integration_celery/blob/master/LICENSE) 
[![HitCount](http://hits.dwyl.io/DataIntegrationAlliance/https://github.com/DataIntegrationAlliance/data_integration_celery.svg)](http://hits.dwyl.io/DataIntegrationAlliance/https://github.com/DataIntegrationAlliance/data_integration_celery)
[![Twitter](https://img.shields.io/twitter/url/https/github.com/DataIntegrationAlliance/data_integration_celery.svg?style=social)](https://twitter.com/intent/tweet?text=Wow:&url=https%3A%2F%2Fgithub.com%2FDataIntegrationAlliance%2Fdata_integration_celery) 

通过celery定期执行更相关任务，将万得wind，同花顺ifind，东方财富choice等数据终端的数据进行整合，清洗，一致化，供其他系统数据分析使用

## 环境依赖及安装配置
+ windows
+ rabbitmq

为了支持独立运行在windows环境下，celery 的 broker 选择 rabbitmq 而非 redis（仅支持linux）

## RabbitMQ 系统配置
### 1. 用户创建及权限配置
创建用户，host，及访问权限
```commandline
rabbitmqctl add_user mg ****
rabbitmqctl set_user_tags mg broker_backend

rabbitmqctl add_vhost celery_tasks
rabbitmqctl set_permissions -p celery_tasks mg ".*" ".*" ".*"

rabbitmqctl add_vhost backend
rabbitmqctl set_permissions -p backend mg ".*" ".*" ".*"
```
### 2. 启动web端
rabbitmq-plugins enable rabbitmq_management

[RabbitMQ 管理界面](http://localhost:15672/#/connections)

## 启动 celery
### 1. 启动 worker
```commandline
celery -A tasks worker --loglevel=info -c 4 -P eventlet
```
> -P 命令只要是为了在win10 下可以正常运行 [issue](https://github.com/celery/celery/issues/4081) 其他环境下可以去除
-c 命令后面的数字表示平行运行的 worker 数量，建议不要超过CPU核数

### 2. 启动 beat

```commandline
celery beat -A tasks
```
CeleryConfig 中的定时任务将通过 beat 自动启动

### 3. Schedules Configuration
推荐配置
```python
from celery.schedules import crontab


class CeleryConfig:
    # Celery settings
    broker_url = 'amqp://mg:***@localhost:5672/celery_tasks',
    result_backend = 'amqp://mg:***@localhost:5672/backend'
    accept_content = ['json']  # , 'pickle'
    timezone = 'Asia/Shanghai'
    imports = ('tasks',)
    beat_schedule = {
        'daily_task': {
            'task': 'tasks.grouped_task_daily',
            'schedule': crontab(hour='16', minute='03', day_of_week='1-5'),
        },
        'weekly_task': {
            'task': 'tasks.grouped_task_weekly',
            'schedule': crontab(hour='10', day_of_week='6'),
        },
    }
    broker_heartbeat = 0
```

## MySQL 配置方法

 1. Ubuntu 18.04 环境下安装 MySQL，5.7
 
    ```bash
    sudo apt install mysql-server
    ```
 2. 默认情况下，没有输入用户名密码的地方，因此，安装完后需要手动重置Root密码，方法如下：

    ```bash
    cd /etc/mysql/debian.cnf
    sudo more debian.cnf
    ```
    出现类似这样的东西
    ```bash
    # Automatically generated for Debian scripts. DO NOT TOUCH!
    [client]
    host     = localhost
    user     = debian-sys-maint
    password = j1bsABuuDRGKCV5s
    socket   = /var/run/mysqld/mysqld.sock
    [mysql_upgrade]
    host     = localhost
    user     = debian-sys-maint
    password = j1bsABuuDRGKCV5s
    socket   = /var/run/mysqld/mysqld.sock
    ```

    以debian-sys-maint为用户名登录，密码就是debian.cnf里那个 password = 后面的东西。
    使用mysql -u debian-sys-maint -p 进行登录。
    进入mysql之后修改MySQL的密码，具体的操作如下用命令：
    ```mysql
    use mysql;
    
    update user set authentication_string=PASSWORD("Dcba4321") where user='root';
    
    update user set plugin="mysql_native_password"; 
     
    flush privileges;
    ```
 3. 然后就可以用过root用户登陆了

    ```bash
    mysql -uroot -p
    ```

 4. 创建用户 mg 默认密码 Abcd1234

    ```mysql
    CREATE USER 'mg'@'%' IDENTIFIED BY 'Abcd1234';
    ```
 5. 创建数据库 md_integration

    ```mysql
    CREATE DATABASE `md_integration` default charset utf8 collate utf8_general_ci;
    ```
 6. 授权

    ```mysql
    grant all privileges on md_integration.* to 'mg'@'localhost' identified by 'Abcd1234'; 
    
    flush privileges; #刷新系统权限表
    ```
 