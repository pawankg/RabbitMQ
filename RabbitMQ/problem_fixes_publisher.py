import pika
import sys
import random

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))

channel = connection.channel()

# Solution: RabbitMQ Provision #1
# This API ensures that whenever some message is sent on this channel, it will receive an acknowledgement back from the
# broker
channel.confirm_delivery()

# Solution: RabbitMQ Provision #2
channel.exchange_declare(exchange='logs_exchange', exchange_type='direct', durable=True)

severity = ["Error", "Warning", "Info", "Other"]
messages = ["EMsg", "WMsg", "IMsg", "OMsg"]

for i in range(10):
    randomNum = random.randint(0, len(severity)-1)
    message = messages[randomNum]
    rk = severity[randomNum]
    try:
        channel.basic_publish(exchange='logs_exchange',
                              routing_key=rk,
                              body=message,
                              properties=pika.BasicProperties(
                                  delivery_mode=2  # Publisher is defining which message to make Persistent
                                  )
                              )
        print("[x] sent %r" % message)

    # Solution: RabbitMQ Provision #1
    # Below exceptions tells us the actual reason of failure of message delivery from publisher to message broker
    except pika.exceptions.ChannelClosed:
        print("Channel Closed")
    except pika.exceptions.ConnectionClosed:
        print("Connection Closed")


channel.exchange_delete(exchange='logs_exchange', if_unused=False)

connection.close()
