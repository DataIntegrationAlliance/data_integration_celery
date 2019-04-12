#! /usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author  : MG
@Time    : 2019/1/18 9:35
@File    : check.py
@contact : mmmaaaggg@163.com
@desc    : 用于检查环境基础组建是否运行正常
"""
import time
from kombu import Connection, Queue
from kombu.mixins import ConsumerMixin
import logging

logger = logging.getLogger()


class C(ConsumerMixin):

    def __init__(self, connection, queueNmae):
        self.connection = connection
        self.queues = [Queue(queueNmae, durable=False)]

    def get_consumers(self, Consumer, channel):
        return [
            Consumer(self.queues, callbacks=[self.on_message]),
        ]

    # 接收处理消息的回调函数
    def on_message(self, body, message):
        logger.info("Received %s" % body)
        message.ack()


def check_rabbit_mq(url):
    """
    'amqp://mg:***@localhost:5672/celery_tasks'
    :param url:
    :return:
    """
    pass


def receiver(url):
    logger.info("start receiver")
    with Connection(url) as conn:
        C(conn, 'queuetest').run()


def sender(url):
    logger.info("start sender")
    with Connection(url) as conn:
        with conn.channel() as channel:
            # producer = Producer(channel)
            producer = channel.Producer()

            while True:
                message = time.strftime('%H:%M:%S', time.localtime())
                producer.publish(
                    body=message,
                    retry=True,
                    exchange='celery',
                    routing_key='rkeytest'
                )
                logger.info('send message: %s' % message)

                while True:
                    # 检查队列，以重新得到消息计数
                    queue = channel.queue_declare(queue='queuetest', passive=True)
                    messageCount = queue.message_count
                    logger.info('messageCount: %d' % messageCount)
                    if messageCount < 100:
                        time.sleep(0.5)
                        break
                    time.sleep(1)


def test(url):
    from kombu import Exchange, Queue, Connection, Consumer, Producer
    task_queue = Queue('tasks', exchange=Exchange('celery', type='direct'), routing_key='tasks')
    # 生产者
    with Connection(url) as conn:
        with conn.channel() as channel:
            producer = Producer(channel)
            producer.publish({'hello': 'world'},
                             retry=True,
                             exchange=task_queue.exchange,
                             routing_key=task_queue.routing_key,
                             declare=[task_queue])

    def get_message(body, message):
        print("receive message: %s" % body)
        # message.ack()

    # 消费者
    with Connection(url) as conn:
        with conn.channel() as channel:
            consumer = Consumer(channel, queues=task_queue, callbacks=[get_message, ], prefetch_count=10)
            consumer.consume(no_ack=True)


if __name__ == "__main__":
    from tasks.config import CeleryConfig
    # sender(CeleryConfig.broker_url)
    # receiver(CeleryConfig.broker_url)
    test(CeleryConfig.broker_url)
