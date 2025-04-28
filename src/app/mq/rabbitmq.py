import pika

from src.config import get_config


def get_rabbitmq_connection():
    config = get_config()['mq']['rabbitmq']
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(
            host=config['host'],
            port=config['port'],
            virtual_host=config['vhost'],
            credentials=pika.PlainCredentials(
                username=config['user'],
                password=config['pass'],
            ),
        )
    )
    channel = connection.channel()
    return connection, channel
