import os
import argparse
import pandas as pd
from time import time, sleep

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_extract, split, explode
from pyspark.sql.types import *
from datetime import datetime, timedelta
from hdfs import InsecureClient
from pyspark.sql.functions import to_date, date_format
from datetime import datetime

from config import *

if __name__ == "__main__":

    parser = argparse.ArgumentParser("Kafka & Spark Streaming for Log Ana Application")
    parser.add_argument("--topic", type=str, required=True, help="kafka consumer topic, please register the same topic name as the producer")
    parser.add_argument("--pyspark_package", type=str, required=False, default="org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1")
    parser.add_argument("--save_dir", type=str, required=False, default="log_analysis", help="directory to save files")
    parser.add_argument("--hdfs", required=False, action="store_true", help="save files to hdfs or local directory")
    parser.add_argument("--hdfs_url", required=False, default="hdfs://localhost:9000", help="hdfs local url")
    parser.add_argument("--log_file", type=str, required=False, default=None, help="default won't save a log file, specify a log filename to save log")
    parser.add_argument("--broker", type=str, required=False, default="127.0.0.1:9092", help="kafka consumer broker")
    parser.add_argument("--group_id", type=str, required=False, default=None, help="kafka consumer group id")
    args = parser.parse_args()

    if not os.path.isdir("./output"):
        os.makedirs("./output")

    logger = myLogger(saveto=os.path.join("output", args.log_file))
    
    os.environ['PYSPARK_SUBMIT_ARGS'] = f'--packages {args.pyspark_package} pyspark-shell'
    logger.warning(f"Make sure to match the correct spark and scala version with --pyspark_package: {args.pyspark_package}")

    now_time = datetime.now()
    now_time = now_time.strftime("%Y_%m_%d_%H_%M_%S")
    save_path = os.path.join(args.hdfs_url if args.hdfs else "", args.save_dir + "_" + now_time, "parquet")
    checkpoint_path = os.path.join(args.hdfs_url if args.hdfs else "", args.save_dir + "_" + now_time, "checkpoints")

    spark = SparkSession.builder.appName('KafkaConsumerSparkStreaming').getOrCreate()

    try:
        spark.sparkContext.setLogLevel("WARN")

        # Define the Kafka source options
        kafka_options = {
            'kafka.bootstrap.servers': args.broker,
            'subscribe': args.topic,
            'startingOffsets': 'earliest',
        }

        if args.group_id:
            kafka_options["group_id"] = args.group_id

        # Read the Kafka stream
        base_df = spark.readStream.format('kafka').options(**kafka_options).load()
        # Convert the binary message value to string
        base_df = base_df.withColumn('value', col('value').cast('string'))

        # Split the batch into individual lines
        base_df = base_df.withColumn('value', explode(split(col('value'), ' \|\| ')))

        # Extract the desired fields from the log line
        logs_df = base_df.select(regexp_extract('value', host_pattern, 1).alias('host'),
                                regexp_extract('value', ts_pattern, 1).alias('timestamp'),
                                regexp_extract('value', method_uri_protocol_pattern, 1).alias('method'),
                                regexp_extract('value', method_uri_protocol_pattern, 2).alias('endpoint'),
                                regexp_extract('value', method_uri_protocol_pattern, 3).alias('protocol'),
                                regexp_extract('value', status_pattern, 1).cast('integer').alias('status'),
                                regexp_extract('value', content_size_pattern, 1).cast('integer').alias('content_size'))
        
        logs_df = logs_df.withColumn('day_of_week', date_format(to_date('timestamp', 'yyyy-MM-dd HH:mm:ss'), 'E'))
        logs_df = logs_df[logs_df['day_of_week'].isNotNull()]

        query = logs_df.writeStream \
                .outputMode('append') \
                .format('parquet') \
                .option('path', save_path) \
                .option('checkpointLocation', checkpoint_path) \
                .trigger(processingTime='10 seconds') \
                .start()
                # .trigger(processingTime='10 seconds') \

        idle_duration = 60  # idle time in seconds
        last_activity_time = time()
        last_processed_rows = 0

        while True:
            sleep(1)
            # Get the latest progress
            progress = query.lastProgress
            if progress:
                if progress['numInputRows'] > last_processed_rows:
                    # If new rows were processed, update last activity time and processed rows count
                    last_activity_time = time()
                    last_processed_rows = progress['numInputRows']
                elif time() - last_activity_time > idle_duration:
                    # If no new rows were processed for more than idle_duration, stop the query
                    query.stop()
                    break

        query.awaitTermination()
        logger.header("Finished consuming messages.")

        # After the streaming is done, read from the Parquet files to a static DataFrame
        static_df = spark.read.parquet(save_path)
        row_count = static_df.count()
        logger.info(f"The DataFrame has {row_count} rows.")

        # # Now, you can perform any transformations/operations on this static DataFrame
        # # For example, find the most popular endpoint for each day of the week
        logger.header("Find the most used endpoint in each day of week")
        highest_day_endpoint = [static_df.filter(static_df['day_of_week'] == day).groupBy("endpoint").count().sort("count", ascending=False).limit(1).toPandas() for day in day_of_week]
        highest_day_endpoint_pd_df = pd.concat(highest_day_endpoint)
        highest_day_endpoint_pd_df.index = day_of_week
        logger.info(f"\n{highest_day_endpoint_pd_df}")
        highest_day_endpoint_pd_df.to_csv("output/highest_day_endpoint.csv")

        logger.header("Calculate the number of 404 status in each day of week")
        is404_df = static_df.filter(static_df["status"] == 404)
        is404_cnt_day_of_week_pd_df = is404_df.groupBy("day_of_week").count().toPandas()
        logger.info(f"\n{is404_cnt_day_of_week_pd_df}")
        is404_cnt_day_of_week_pd_df.to_csv("output/is404_cnt_day_of_week.csv")

    except Exception as e:
        print(e)
    finally:
        spark.stop()