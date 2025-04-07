import time
from threading import Thread

import pika

# Connect to RabbitMQ Server
credentials = pika.PlainCredentials('admin', 'admin')

# Callback functions for corresponding queue callbacks
def callback_function_for_queue_a(ch, method, properties, body):
    print(f'Got message from Queue A : {body}')

def callback_function_for_queue_b(ch, method, properties, body):
    print(f'Got message from Queue B : {body}')

def callback_function_for_queue_c(ch, method, properties, body):
    print(f'Got message from Queue C : {body}')

def start_consumer():
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(
            host='192.168.1.5',
            port=5672,
            credentials=credentials))

        channel = connection.channel()

        channel.exchange_declare(exchange='test', durable=True, exchange_type='topic')

        channel.queue_declare(queue='A_QUEUE')
        channel.queue_declare(queue='B_QUEUE')
        channel.queue_declare(queue='C_QUEUE')

        channel.queue_bind(exchange='test', queue='A_QUEUE', routing_key='A')
        channel.queue_bind(exchange='test', queue='B_QUEUE', routing_key='B')
        channel.queue_bind(exchange='test', queue='C_QUEUE', routing_key='C')

        # setup consumer
        channel.basic_consume(queue='A_QUEUE', on_message_callback=callback_function_for_queue_a, auto_ack=True)
        channel.basic_consume(queue='B_QUEUE', on_message_callback=callback_function_for_queue_b, auto_ack=True)
        channel.basic_consume(queue='C_QUEUE', on_message_callback=callback_function_for_queue_c, auto_ack=True)

        print('Starting Consumer... [For Exit : C-c]')
        channel.start_consuming()
    except Exception as ex:
        print(f'Consumer Exception : {ex}')
        if 'connection' in locals() and not connection.is_closed:
            connection.close()
        time.sleep(5)

if __name__ == '__main__':
    consumer_thread = Thread(target=start_consumer)
    consumer_thread.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print(f'Consumer Stopped!')
        exit(1)


