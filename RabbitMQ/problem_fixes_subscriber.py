import pika
import random
import time


subId = random.randint(1, 100)
print("Subscriber Id = ", subId)


connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))

channel = connection.channel()

# Solution: RabbitMQ Provision #2
channel.exchange_declare(exchange='logs_exchange', exchange_type='direct', durable=True)

queue_name = "task_queue"

# Solution: RabbitMQ Provision #3
result = channel.queue_declare(queue=queue_name,
                               # exclusive=True,          # Solution: RabbitMQ Provision #2
                               durable=True)

severity = ["Error", "Warning", "Info", "Other"]

for s in severity:
    channel.queue_bind(exchange='logs_exchange', queue=queue_name, routing_key=s)

print('[*] waiting for the messages')


def callback(ch, method, properties, body):
    print('[x] Received message:::: %r' % body)
    # randomSleep = random.randint(1, 5)
    randomSleep = 5
    print("Working for ", randomSleep, "seconds")
    while randomSleep > 0:
        print(".", end="")
        time.sleep(1)
        randomSleep -= 1
    print("!")

    # Solution: RabbitMQ Provision #4
    # When a message is send from the queue to the subscriber it is sent with a delivery_tag which should be send in ack
    ch.basic_ack(delivery_tag=method.delivery_tag)


# By setting the prefetch_count=1, the queue will not send any message after sending one message until it receives the
# acknowledgement from the subscriber. So the messages are not more distributed in round robin fashion infact they are
# distributed depending on the load of the subscriber.
channel.basic_qos(prefetch_count=1)
channel.basic_consume(queue=queue_name,
                      on_message_callback=callback
                      # auto_ack=True
                      )

channel.start_consuming()
