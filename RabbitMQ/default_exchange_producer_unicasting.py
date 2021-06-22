import pika

# Create a connection, say CN
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))  # In parenthesis, ip address is reqd.
# Create a channel in the connection (CN), say CH
channel = connection.channel()          # Here, the object of connection is used to create channel

# Create an Exchange and Specify the bindings
# This step is not required in this example, as we are working with default exchange

# If the queue does not exist already, Create a queue through the channel.
channel.queue_declare(queue="hello")     # Here, the queue name is hello

# Publish the message to the exchange
# exchange="" since we are using default exchange
# routing_key should be exactly same as the queue_name
channel.basic_publish(exchange="", routing_key="hello", body="hello_world 2")
print("[x] Sent Hello World")

# Close the connection
# Automatically closes the channel
connection.close()
