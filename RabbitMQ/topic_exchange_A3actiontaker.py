import pika

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))

channel = connection.channel()

# exchange_type='topic', to use the topic exchange
channel.exchange_declare(exchange='system_exchange', exchange_type='topic')

result = channel.queue_declare(queue='', exclusive=True)

queue_name = result.method.queue

# print("Subscriber queue_name =", queue_name)

# Routing key format we are using here: <Severity>.<Priority>.<Action>.<Component>
# routing_key="#.A3.#" means # matches any severity, priority action done by A3 belonging to any component.
channel.queue_bind(exchange='system_exchange', queue=queue_name, routing_key="#.A3.#")

print('[*] waiting for the messages')


def callback(ch, method, properties, body):
    print('[x] :::: %r' % body)


channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)

channel.start_consuming()
