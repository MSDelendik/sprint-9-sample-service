import time
from datetime import datetime
from logging import Logger
from stg_loader.repository.stg_repository import StgRepository
from lib.kafka_connect.kafka_connectors import KafkaConsumer
from lib.kafka_connect.kafka_connectors import KafkaProducer
from lib.redis.redis_client import RedisClient
from lib.pg import pg_connect
import json


class StgMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 producer: KafkaProducer,
                 redis: RedisClient,
                 stg_repository: StgRepository,
                 batch_size: int,
                 logger: Logger) -> None:
        self._logger = logger
        self._consumer = consumer
        self._producer = producer
        self._redis = redis
        self._stg_repository = stg_repository
        self._batch_size = batch_size

    # функция, которая будет вызываться по расписанию.
    def run(self) -> None:
        # Пишем в лог, что джоб был запущен.
        self._logger.info(f"{datetime.utcnow()}: START")

        # Имитация работы. Здесь будет реализована обработка сообщений.
        for i in range(self._batch_size):
            msg = self._consumer.consume()
            print(msg)
            if not msg:
                self._logger.info(f"{datetime.utcnow()}: There is no message")
                break

            payload = msg.get('payload')

            self._stg_repository.order_events_insert(msg['object_id'],
                                                     msg['object_type'],
                                                     msg['sent_dttm'],
                                                     json.dumps(payload)
                                                     )
            user_id = payload['user']['id']
            user = self._redis.get(user_id)

            restaurant_id = payload['restaurant']['id']
            restaurant = self._redis.get(restaurant_id)

            products = []

            for product in payload.get('order_items'):
                for i in restaurant['menu']:
                    if product['id'] == i['_id']:
                        category = i['category']

                products.append(
                    {
                        'id': product.get('id'),
                        'price': product.get('price'),
                        'quantity': product.get('quantity'),
                        'name': product.get('name'),
                        'category': category
                    }
                )

            out_message = {
                'object_id': msg['object_id'],
                'object_type': msg['object_type'],
                # 'sent_dttm': msg['sent_dttm'],
                'payload': {
                    'id': msg['object_id'],
                    'date': payload['date'],
                    'cost': payload['cost'],
                    'payment': payload['payment'],
                    'status': payload['final_status'],
                    'restaurant': {
                        'id': restaurant_id,
                        'name': restaurant['name'],
                    },
                    'user': {
                        'id': user_id,
                        'name': user['name']
                    },
                    'products': products
                }
            }

            self._producer.produce(out_message)

        # Пишем в лог, что джоб успешно завершен.
        self._logger.info(f"{datetime.utcnow()}: FINISH")
