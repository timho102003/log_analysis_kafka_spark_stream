import os
import argparse
from confluent_kafka import Producer

from config import *


def generate_receipt(index):
    def delivery_report(err, msg):
        if err is not None:
            print(f'Index: {index}, Message delivery failed: {err}')
        else:
            print(f'Index: {index}, Message delivered to {msg.topic()} [{msg.partition()}]')
    return delivery_report


if __name__ == "__main__":

    parser = argparse.ArgumentParser("Feed Log File to Kafka Producer Application")
    parser.add_argument("--file", type=str, required=True, help="Dataset file path")
    parser.add_argument("--batch", type=int, required=False, default=1000, help="number of rows per batch")
    parser.add_argument("--log_file", type=str, required=False, default=None, help="default won't save a log file, specify a log filename to save log")
    parser.add_argument("--broker", type=str, required=False, default="127.0.0.1:9092", help="kafka producer broker")
    parser.add_argument("--topic", type=str, required=False, default="logs_ana_topic", help="kafka producer topic")
    
    args = parser.parse_args()

    if not os.path.isdir("./output"):
        os.makedirs("./output")

    logger = myLogger(saveto=os.path.join("output", args.log_file))    
    
    if not os.path.isfile(args.file):
        logger.fail(msg="Dataset {} is not a valid file path!")
        raise FileNotFoundError

    # Set up the Kafka producer
    producer = Producer({
        'bootstrap.servers': args.broker,  # Kafka broker address
        'api.version.request': True,
    })

    batch_size = args.batch  # Adjust this value based on requirements

    logger.header(
        f"""
        Feed log file to producer:
        - Dataset: {args.file}
        - Batch Size: {batch_size}
        - Producer Broker: {args.broker}
        - Producer Topic: {args.topic}
        """
    )

    # Open the log file
    with open(args.file, 'r') as file:
        batch = []
        index = 0
        for line in file:
            # Add each line to the current batch
            batch.append(line.strip())

            # Check if the batch size is reached
            if len(batch) >= batch_size:
                # Produce the batch to Kafka
                producer.produce(args.topic, value=' || '.join(batch).encode('utf-8'), callback=generate_receipt(index))
                producer.flush()

                # Clear the batch
                batch = []
                index += 1
        
        # Check if there are any remaining lines in the last batch
        # Produce the remaining lines to Kafka
        producer.poll(1)
        if batch:
            producer.produce(args.topic, ' || '.join(batch), callback=generate_receipt(index))
        producer.flush()

    # Flush any remaining messages in the producer queue
    producer.produce(args.topic, key= "Stop",value="Bye....",callback=generate_receipt(index))
    producer.flush()