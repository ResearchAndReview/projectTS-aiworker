import logging
from src import config
from src.app.db.mysql import get_mysql_connection
from src.app.mq.rabbitmq import get_rabbitMQ_connection


def callback(ch, method, properties, body):
    print(f"수신한 메시지 : {body.decode()}")

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
    mqconn, mqchannel = get_rabbitMQ_connection()
    mqchannel.queue_declare(queue='task.queue.test', durable=True)
    mqchannel.basic_consume(
        queue='task.queue.test',
        on_message_callback=callback,
        auto_ack=True
    )
    mqchannel.start_consuming()


if __name__ == "__main__":
    main()