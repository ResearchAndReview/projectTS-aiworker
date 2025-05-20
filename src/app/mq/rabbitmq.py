import traceback

import pika
import json
import time
import requests
import logging

from src.config import get_config

def handle_ocr_task(message_body):
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

def handle_trans_task(message_body):
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

def handle_test(ch):
    print("LL")
    send_message(ch)

def callback(ch, method, properties, body):
    try:
        message_body = json.loads(body.decode())
        print(f"메시지 수신됨: {message_body}")

        if message_body['taskType'] == 0: # OCR
            handle_ocr_task(message_body)
        elif message_body['taskType'] == 1: # 번역
            handle_trans_task(message_body)
        elif message_body['taskType'] == 99:
            handle_test(ch)
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


def send_message(mqchannel):
    message = {
        'taskType': 0,
        'ocrTaskId': 'test',
        'imageData': 'test'
    }

    mqchannel.queue_declare(queue='task.queue.sendtest', durable=True)

    mqchannel.basic_publish(
        exchange='',
        routing_key='task.queue.sendtest',
        body=json.dumps(message),
        properties=pika.BasicProperties(
            delivery_mode=2,  # 메시지를 영구적으로 저장
        )
    )