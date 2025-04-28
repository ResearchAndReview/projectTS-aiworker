import logging
from src import config
from src.app.db.mysql import get_mysql_connection
from src.app.mq.rabbitmq import get_rabbitmq_connection
import requests

def callback(ch, method, properties, body):
    print(f"메시지 수신됨")
    payload = {
        'image': body.decode()
    }
    response = requests.post("https://imageocrtranslation-114606214163.asia-northeast3.run.app/ocr", json=payload)

    ocr_json = response.json()

    print(f"OCR 응답 수신 {ocr_json}")
    payload = {
        'originalText': ocr_json['captions'][0]['text'],
        "translateFrom": "일본어",
        "translateTo": "한국어"
    }

    response2 = requests.post("https://imageocrtranslation-114606214163.asia-northeast3.run.app/translation", json=payload)
    print(f"번역 응답 수신")
    translation_json = response2.json()

    print(f"번역된 텍스트: {translation_json}")





def main():
    config.load_config()
    logging.info("AI Task Distributor for projectTS initiated")
    sqlconn = get_mysql_connection()

    try:
        with sqlconn.cursor() as cursor:
            sql = "SELECT * FROM Task"
            cursor.execute(sql)

            results = cursor.fetchall()
            for row in results:
                print(row)

    finally:
        sqlconn.close()
    mqconn, mqchannel = get_rabbitmq_connection()
    mqchannel.queue_declare(queue='task.queue.test', durable=True)
    mqchannel.basic_consume(
        queue='task.queue.test',
        on_message_callback=callback,
        auto_ack=True
    )
    mqchannel.start_consuming()


if __name__ == "__main__":
    main()