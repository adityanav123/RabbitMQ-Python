import pika

# credentials
credentials = pika.PlainCredentials('admin', 'admin')

# Connection to the RabbitMQ Server
connection = pika.BlockingConnection(pika.ConnectionParameters(host='192.168.1.5', credentials=credentials))

# Creating Channel for Communication
channel = connection.channel()

# Bridge Name : test : to be declared so that the queues can be accessed
channel.exchange_declare(exchange='test', durable=True, exchange_type='topic')

# declare queues
channel.queue_declare(queue='A_QUEUE')
channel.queue_bind(exchange='test', queue='A_QUEUE', routing_key='A')

channel.queue_declare(queue='B_QUEUE')
channel.queue_bind(exchange='test', queue='B_QUEUE', routing_key='B')

channel.queue_declare(queue='C_QUEUE')
channel.queue_bind(exchange='test', queue='C_QUEUE', routing_key='C')

print("Send messages to queue (type 'quit()' to exit)")
print("Available queues: A, B, C")

while True:
    queue = input("Select queue (A/B/C): ").strip().upper()

    if queue == "QUIT()":
        break

    if queue not in ["A", "B", "C"]:
        print("Invalid queue. Please choose A, B, or C")
        continue

    message = input(f"Enter message for Queue '{queue}': ")

    if message.lower() == "quit()":
        break

    # Send the message to the selected queue
    channel.basic_publish(exchange='test', routing_key=queue, body=message)
    print(f"Message sent to Queue '{queue}'")

# Closing the Communication channel
channel.close()
# Closing the connection
connection.close()
print("Connection closed. Goodbye!")