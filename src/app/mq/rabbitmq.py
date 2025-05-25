import traceback

import pika
import json
import time
import requests
import logging

from src.app.algorithm.node_select import select_node_for_ocr, select_node_for_trans
from src.config import get_config

def handle_ocr_task(ch, message_body):
    target_node = select_node_for_ocr()
    send_message_to_node(ch, target_node['id'], message_body)


def handle_trans_task(ch, message_body):
    target_node = select_node_for_trans()
    send_message_to_node(ch, target_node['id'], message_body)

def _handle_ocr_task(message_body):
    payload = {
        'image': message_body['imageData']
    }
    response = requests.post("https://imageocrtranslation-114606214163.asia-northeast3.run.app/ocr", json=payload)

    ocr_json = response.json()
    print(f"OCR 응답 수신 {ocr_json}")
    if 'error' in ocr_json:
        notify_ocr_failed_response = requests.post(
            f"https://js.thxx.xyz/task/notify/ocr-failed?ocrTaskId={message_body['ocrTaskId']}", json=ocr_json)
        return
    notify_ocr_success_response = requests.post(
        f"https://js.thxx.xyz/task/notify/ocr-success?ocrTaskId={message_body['ocrTaskId']}", json=ocr_json)

    print(notify_ocr_success_response)

def _handle_trans_task(message_body):
    payload = {
        'originalText': message_body['originalText'],
        'translateFrom': '일본어',
        'translateTo': '한국어'
    }

    response2 = requests.post("https://imageocrtranslation-114606214163.asia-northeast3.run.app/translation",
                              json=payload)
    print(f"번역 응답 수신")
    translation_json = response2.json()
    print(f"번역된 텍스트: {translation_json}")

    if 'error' in translation_json:
        notify_trans_failed_response = requests.post(
            f"https://js.thxx.xyz/task/notify/trans-failed?transTaskId={message_body['transTaskId']}",
            json=translation_json)
        return
    payload = {
        'translatedText': translation_json['translatedText'],
    }
    requests.post(f"https://js.thxx.xyz/task/notify/trans-success?transTaskId={message_body['transTaskId']}",
                  json=payload)

def callback(ch, method, properties, body):
    try:
        message_body = json.loads(body.decode())
        # print(f"메시지 수신됨: {message_body}")

        if message_body['taskType'] == 0: # OCR
            handle_ocr_task(ch, message_body)
        elif message_body['taskType'] == 1: # 번역
            handle_trans_task(ch, message_body)
        elif message_body['taskType'] == 99:
            pass
            #handle_test(ch)
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
            print(e)


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