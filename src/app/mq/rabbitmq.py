import json
import logging
import traceback

import pika

from src.app.algorithm.node_select import select_node_for_ocr, select_node_for_trans
from src.config import get_config


def handle_ocr_task(ch, message_body):
    target_node = select_node_for_ocr()
    send_message_to_node(ch, target_node['id'], message_body)


def handle_trans_task(ch, message_body):
    target_node = select_node_for_trans()
    send_message_to_node(ch, target_node['id'], message_body)


def callback(ch, method, properties, body):
    try:
        message_body = json.loads(body.decode())

        if message_body['taskType'] == 0:  # OCR
            logging.info(f"{message_body['ocrTaskId']} OCR 작업 수신됨")
            handle_ocr_task(ch, message_body)
        elif message_body['taskType'] == 1:  # 번역
            logging.info(f"{message_body['transTaskId']} 번역 작업 수신됨")
            handle_trans_task(ch, message_body)
        elif message_body['taskType'] == 99:
            pass
            # handle_test(ch)
    except Exception as e:
        traceback.print_exc()


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


def keep_consuming(mqchannel):
    while True:
        try:
            mqchannel.queue_declare(queue='task.queue.test', durable=True)
            mqchannel.basic_consume(
                queue='task.queue.test',
                on_message_callback=callback,
                auto_ack=True
            )
            mqchannel.start_consuming()
        except Exception as e:
            logging.error(e)


def send_message_to_node(mqchannel, node_id, task):
    queue_name = f'node.{node_id}'
    mqchannel.queue_declare(queue=queue_name, durable=True)
    mqchannel.basic_publish(
        exchange='',
        routing_key=queue_name,
        body=json.dumps(task),
        properties=pika.BasicProperties(
            delivery_mode=2,  # 메시지를 영구적으로 저장
        )
    )
