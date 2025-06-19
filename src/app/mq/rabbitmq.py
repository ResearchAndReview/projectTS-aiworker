import base64
import io
import json
import logging
import traceback

import pika
from PIL import Image

from src.app.algorithm.node_select import select_node_for_ocr, select_node_for_trans
from src.config import get_config
from src.app.db.mysql import increase_ocr_task_size, increase_trans_task_size


def handle_ocr_task(ch, message_body):
    base64_string = message_body['imageData']
    if ',' in base64_string:
        header, base64_data = base64_string.split(',', 1)
        print(f"데이터 URI 헤더 발견: {header}")
        # 필요하다면 header에서 MIME 타입 등을 파싱할 수 있습니다.
        # e.g., mime_type = header.split(';')[0].split(':')[1]
    else:
        # 순수 Base64 문자열이라고 가정
        base64_data = base64_string
    # 3. Base64 문자열을 바이너리 데이터로 디코딩
    image_bytes = base64.b64decode(base64_data)
    decoded_size = len(image_bytes)
    print(f"Base64 디코딩 완료, 크기: {decoded_size} bytes")

    img = Image.open(io.BytesIO(image_bytes))
    img.verify()  # 이미지 데이터 유효성 검사
    # verify() 후에는 다시 열어야 실제 작업 가능
    img = Image.open(io.BytesIO(image_bytes))

    target_node = select_node_for_ocr((img.width * img.height) ** 1.5)
    increase_ocr_task_size(target_node['id'], (img.width * img.height) ** 1.5)
    send_message_to_node(ch, target_node['id'], message_body)


def handle_trans_task(ch, message_body):
    text = message_body['originalText']
    target_node = select_node_for_trans(len(text) ** 1.5)
    increase_trans_task_size(target_node['id'], len(text) ** 1.5)
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
    logging.info(f"{node_id} 노드로 작업 전송")
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
