import json
import logging
import time

from src import config
from src.app.db.mysql import get_mysql_connection
from src.app.mq.rabbitmq import get_rabbitmq_connection
import requests


def callback(ch, method, properties, body):
    try:
        print(f"메시지 수신됨")
        message_body = json.loads(body.decode())

        start_time = time.time()


        payload = {
            'image': message_body['imageData']
        }
        response = requests.post("https://imageocrtranslation-114606214163.asia-northeast3.run.app/ocr", json=payload)

        end_time = time.time()


        ocr_json = response.json()
        elapsed_time = end_time - start_time
        print(f"OCR 응답 수신 {ocr_json} 걸린시간 : {elapsed_time:.3f} 초")
        if 'error' in ocr_json:
            notify_ocr_failed_response = requests.post(f"https://js.thxx.xyz/task/notify/ocr-failed?ocrTaskId={message_body['ocrTaskId']}", json=ocr_json)
            return
        notify_ocr_success_response = requests.post(f"https://js.thxx.xyz/task/notify/ocr-success?ocrTaskId={message_body['ocrTaskId']}", json=ocr_json)

        ocr_success_response_json = notify_ocr_success_response.json()

        print(ocr_success_response_json)
        trans_task_ids = ocr_success_response_json['createdTransTaskId']
        trans_task_ids_index = 0

        for item in ocr_json['captions']:
            payload = {
                'originalText': item['text'],
                'translateFrom': '일본어',
                'translateTo': '한국어'
            }

            response2 = requests.post("https://imageocrtranslation-114606214163.asia-northeast3.run.app/translation", json=payload)
            print(f"번역 응답 수신")
            translation_json = response2.json()
            print(f"번역된 텍스트: {translation_json}")

            if 'error' in translation_json:
                notify_trans_failed_response = requests.post(
                    f"https://js.thxx.xyz/task/notify/trans-failed?taskId={message_body['taskId']}&transTaskId={trans_task_ids[trans_task_ids_index]}", json=translation_json)
                return
            payload = {
                'translatedText': translation_json['translatedText'],
            }
            requests.post(f"https://js.thxx.xyz/task/notify/trans-success?transTaskId={trans_task_ids[trans_task_ids_index]}", json=payload)
            trans_task_ids_index += 1
    except Exception as e:
        print(e)



def main():
    config.load_config()
    logging.info("AI Task Distributor for projectTS initiated")
    sqlconn = get_mysql_connection()

    try:
        with sqlconn.cursor() as cursor:
            sql = "SELECT * FROM Task ORDER BY createdAt DESC LIMIT 3"
            cursor.execute(sql)

            results = cursor.fetchall()
            for row in results:
                print(row)

    finally:
        sqlconn.close()

    while True:
        try:
            mqconn, mqchannel = get_rabbitmq_connection()
            mqchannel.queue_declare(queue='task.queue.test', durable=True)
            mqchannel.basic_consume(
                queue='task.queue.test',
                on_message_callback=callback,
                auto_ack=True
            )
            mqchannel.start_consuming()
        except Exception as e:
            print(e)


if __name__ == "__main__":
    main()