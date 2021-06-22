import pika, sys, os

def main():
    # Create a connection, say CN
    connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
    # Create a channel in the connection (CN), say CH
    channel = connection.channel()
    # If the queue does not exist already, Create a queue through the channel.
    channel.queue_declare(queue="hello")

    # A callback function is created so that whenever a message is posted to the queue, this function is executed
    def callback(ch, method, properties, body):
        # printing the incoming message
        print("[x] received %r" % body)

    # Associate the callback function with the message queue
    # on_message_callback= name of the function
    # As soon as the subscriber is receiving the message from the queue, it acknowledges it back to the queue so that
    # the queue can delete this particular message from itself. Otherwise message will keep on adding in the queue
    channel.basic_consume(queue="hello", on_message_callback=callback, auto_ack=True)

    print(" [*] waiting for the messages. To exit press Ctrl-C")
    # Start consuming the messages. It is a blocking call and the program will go into a loop and keeps on listening the
    # queue. As soon as the message is received in the queue, the corresponding callback function will get executed
    channel.start_consuming()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Interrupted")
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)

