import pika

from src.config import get_config


def get_rabbitmq_connection():
    config = get_config()
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(
            host=config['mq']['rabbitmq']['host'],
            port=config['mq']['rabbitmq']['port'],
            virtual_host=config['mq']['rabbitmq']['vhost'],
            credentials=pika.PlainCredentials(
                username=config['mq']['rabbitmq']['user'],
                password=config['mq']['rabbitmq']['pass'],
            ),
        )
    )
    channel = connection.channel()
    return connection, channel
