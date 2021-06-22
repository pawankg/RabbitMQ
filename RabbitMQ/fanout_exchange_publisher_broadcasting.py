import pika
import sys

# Create a connection, say CN
# Create a channel in CN, say CH
# Create an Exchange
# Publish the message
# Close the connection
# Automatically closes the channel

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))

channel = connection.channel()

channel.exchange_declare(exchange='br_exchange', exchange_type='fanout')

for i in range(4):
    message = "Hello" + str(i)
    channel.basic_publish(exchange='br_exchange', routing_key='', body=message)
    print("[x] sent %r" % message)

# Deleting the exhange using the channel object
# if_unused=True, the exchange will be deleted only if there are no queues left which are associated with the exchange
# if_unused=False, the exchange will be deleted even if queues are left which are associated with the exchange
channel.exchange_delete(exchange='br_exchange', if_unused=False)

connection.close()
