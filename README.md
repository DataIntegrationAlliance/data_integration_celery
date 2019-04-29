# data_integration_celery 
[![Build Status](https://travis-ci.org/DataIntegrationAlliance/data_integration_celery.svg?branch=master)](https://travis-ci.org/DataIntegrationAlliance/data_integration_celery)
[![GitHub issues](https://img.shields.io/github/issues/DataIntegrationAlliance/data_integration_celery.svg)](https://github.com/DataIntegrationAlliance/data_integration_celery/issues)
[![GitHub forks](https://img.shields.io/github/forks/DataIntegrationAlliance/data_integration_celery.svg)](https://github.com/DataIntegrationAlliance/data_integration_celery/network)
[![GitHub stars](https://img.shields.io/github/stars/DataIntegrationAlliance/data_integration_celery.svg)](https://github.com/DataIntegrationAlliance/data_integration_celery/stargazers) 
[![GitHub license](https://img.shields.io/github/license/DataIntegrationAlliance/data_integration_celery.svg)](https://github.com/DataIntegrationAlliance/data_integration_celery/blob/master/LICENSE) 

通过celery定期执行更相关任务，将万得wind，同花顺ifind，东方财富choice等数据终端的数据进行整合，清洗，一致化，供其他系统数据分析使用

为了满足不同环境需要，也支持不使用celery，直接运行

# 目录：
+ [环境依赖及安装配置](#1)
+ [首次运行前环境配置](#2)
+ [RabbitMQ 系统配置](#3)
+ [Window CMD 启动](#4)
+ [celery 启动方法](#5)
+ [MySQL 配置方法](#6)

## <span id="1">环境依赖及安装配置</span>
+ windows，ubuntu均可
+ rabbitmq
+ python 3.6 及相关包
+ mysql 5.7

> 如果需要下载 windy、ifind、choice 等数据，需要安装对应的组建

> 为了支持独立运行在windows环境下，celery 的 broker 选择 rabbitmq 而非 redis（仅支持linux）

## <span id="2">首次运行前环境配置</span>
项目全部的配置信息存放在 ./tasks/config.py 文件中
包括： 
+ Celery 配置信息
+ 数据库配置信息
+ IFind、Wind、Tushare、JQDataSDK、CMC等用户名密码配置信息
+ 是否支持 SQLite导出功能及路经配置信息
+ 日志输出格式及级别配置信息

## <span id="3">RabbitMQ 系统配置</span>
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

##  <span id="4">Window CMD 启动</span>
以下命令均才 data_integration_celery 根目录下运行
```commandline
scripts\run.bat
```

输出界面如下：

> run: Active Env[A] Worker[W] Beat[B] Local Tasks[L] Cancel[C] [A,W,B,L,C]?

A：激活虚拟环境（如果有的话） \
W：启动 worker \
B：启动 beat \
L：启动本地运行 \
C：退出

> 其中，“L：启动本地运行”，将启动python程序进入选择界面，相关运行代码在 tasks\__init__.py 文件 main() 方法中。
如果有进一步希望进行自己的定制，可以根据需要，增加 func_list 中的函数列表

## <span id="5">celery 启动方法 </span>
以下命令均才 data_integration_celery 根目录下运行
### 1. 启动 worker
```commandline
celery -A tasks worker --loglevel=debug -c 2 -P eventlet
```
> -P 命令只要是为了在win10 下可以正常运行 [详见 issue](https://github.com/celery/celery/issues/4081)，其他环境下可以去除 \
-P, --pool Pool implementation: prefork (default), eventlet, gevent or solo. \
-c 命令后面的数字表示平行运行的 worker 数量，建议不要超过CPU核数 \
-l, --loglevel Logging level, choose between DEBUG, INFO, WARNING, ERROR, CRITICAL, or FATAL. 

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

> 具体命令及执行时间可根据需要进行配置

## <span id="6">MySQL 配置方法</span>

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
 
 