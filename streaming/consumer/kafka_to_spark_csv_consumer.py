import os

from pyspark.sql.functions import *

from insurance_exception.insurance_exception import  InsuranceException as KafkaToSparkCSVConsumerException
import sys
from utility import read_params
from streaming.transformer.spark_transformer import SparkTransformer


class KafkaToSparkCSVConsumer:
    def __init__(self, schema_string, database_name, collection_name, spark_session, processing_interval_second=5,
                 config_path=None, ):
        try:
            # accepting default configuration file if no configuration file path has been specified during object
            # instantiation
            path = os.path.join("config", "params.yaml") if config_path is None else os.path.join(config_path)
            self.config = read_params(config_path=path)
            self.kafka_topic_name = self.config['kafka']['topic_name']
            self.kafka_bootstrap_server = self.config['kafka']['kafka_bootstrap_server']
            self.spark_session = spark_session
            self.schema = schema_string  # "age INT,sex STRING,bmi DOUBLE,children INT,smoker STRING,region STRING"
            self.spark_transformer = SparkTransformer(database_name=database_name, collection_name=collection_name)
            self.processing_interval_second = processing_interval_second
            self.query = None
        except Exception as e:
            kafka_to_csv_consumer_exception = KafkaToSparkCSVConsumerException(
                "Error occurred  in module [{0}] class [{1}] method [{2}] ".
                    format(self.__module__, KafkaToSparkCSVConsumer.__name__,
                           self.__init__.__name__))
            raise Exception(kafka_to_csv_consumer_exception.error_message_detail(str(e), sys)) from e

    def receive_csv_data_from_kafka_topics(self):
        try:
            dataframe = self.spark_session \
                .readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", self.kafka_bootstrap_server) \
                .option("subscribe", self.kafka_topic_name) \
                .option("startingOffsets", "latest") \
                .load()
            dataframe_1 = dataframe.selectExpr("CAST(value as STRING) ", "timestamp")
            dataframe_2 = dataframe_1.select(from_csv(col("value"), self.schema).alias("records"), "timestamp")
            dataframe_3 = dataframe_2.select("records.*", "timestamp")
            transformed_df = dataframe_3
            for transformer in self.spark_transformer.ml_transformer:
                transformed_df = transformer.transform(transformed_df)
            self.query = transformed_df.writeStream.trigger(
                processingTime=f'{self.processing_interval_second} seconds').foreachBatch(
                self.spark_transformer.process_each_record).start()
            self.query.awaitTermination()
        except Exception as e:
            kafka_to_csv_consumer_exception = KafkaToSparkCSVConsumerException(
                "Error occurred  in module [{0}] class [{1}] method [{2}] ".
                    format(self.__module__, KafkaToSparkCSVConsumer.__name__,
                           self.receive_csv_data_from_kafka_topics.__name__))
            raise Exception(kafka_to_csv_consumer_exception.error_message_detail(str(e), sys)) from e

    def stop_stream(self):
        try:
            if self.query is not None:
                self.query.stop()

        except Exception as e:
            kafka_to_csv_consumer_exception = KafkaToSparkCSVConsumerException(
                "Error occurred  in module [{0}] class [{1}] method [{2}] ".
                    format(self.__module__, KafkaToSparkCSVConsumer.__name__,
                           self.receive_csv_data_from_kafka_topics.__name__))
            raise Exception(kafka_to_csv_consumer_exception.error_message_detail(str(e), sys)) from e
