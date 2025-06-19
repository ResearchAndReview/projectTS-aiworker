import logging

from src import config
from src.app.mq.rabbitmq import get_rabbitmq_connection, keep_consuming


def main():
    config.load_config()
    logging.info("AI Task Distributor for projectTS initiated")

    mqconn, mqchannel = get_rabbitmq_connection()

    keep_consuming(mqchannel)


if __name__ == "__main__":
    main()
