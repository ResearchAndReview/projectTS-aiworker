import pika

def get_rabbitMQ_connection():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(
            host='#####',
            port=0000,
            virtual_host='######',
            credentials=pika.PlainCredentials(
                username='#####',
                password='######'
            ),
        )
    )
    channel = connection.channel()
    return connection, channel
