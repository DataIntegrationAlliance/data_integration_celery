# data_integration_celery
通过celery定期执行更相关任务，将万得wind，同花顺ifind，东方财富choice等数据终端的数据进行整合，清洗，一致化，供其他系统数据分析使用

### 环境依赖
+ windows
+ rabbitmq

为了支持独立运行在windows环境下，celery 的 broker 选择 rabbitmq 而非 redis（仅支持linux）

