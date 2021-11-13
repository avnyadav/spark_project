import os
import sys

from kafka import KafkaProducer
from utility import read_params
import time
from insurance_exception.insurance_exception import InsuranceException as KafkaCSVDataProducerException
from streaming.spark_manager.spark_manager import SparkManager


class KafkaCSVDataProducer:

    def __init__(self, spark_session,config_path=None):
        """
        Creator:
        **********************************************************************************************************
        created date: 02 November 2021
        Organization: iNeuron
        author: avnish@ineuron.ai
        **********************************************************************************************************
        Description:
        **********************************************************************************************************
        KafkaCSVDataProducer is responsible to read a csv file and send data row by row to a kafka topic specified in
        configuration file:
        define below record
        kafka:
          topic_name:<topic-name>
          kafka_bootstrap_server: <server:port>
        *************************************************************************************************************
        Example:
        kafka:
          topic_name: insurance-prediction
          kafka_bootstrap_server: localhost:9092

        parameters:
        =============================================================================================================
        param config_path: configuration file path default is config/param.yaml

        """
        try:
            # accepting default configuration file if no configuration file path has been specified during object
            # instantiation
            path = os.path.join("config", "params.yaml") if config_path is None else os.path.join(config_path)
            self.config = read_params(config_path=path)
            self.kafka_topic_name = self.config['kafka']['topic_name']
            self.kafka_bootstrap_server = self.config['kafka']['kafka_bootstrap_server']
            # creating kafka producer object
            self.kafka_producer = KafkaProducer(bootstrap_servers=self.kafka_bootstrap_server,
                                                value_serializer=lambda x: x.encode('utf-8'))
            # obtain spark session object
            self.spark_session = spark_session
        except Exception as e:
            kafka_csv_data_producer_exp = KafkaCSVDataProducerException(
                "Error occurred  in module [{0}] class [{1}] method [{2}] ".
                    format(self.__module__, KafkaCSVDataProducer.__name__,
                           self.__init__.__name__))
            raise Exception(kafka_csv_data_producer_exp.error_message_detail(str(e), sys)) from e

    def send_csv_data_to_kafka_topic(self, directory_path):
        """
        Creator:
        **********************************************************************************************************
        created date: 02 November 2021
        Organization: iNeuron
        author: avnish@ineuron.ai
        **********************************************************************************************************
        Description:
        **********************************************************************************************************
        function will send all csv files content to kafka topics specified in configuration file.
        ==========================================================================================================
        param:
        directory_path: csv file directory

        ==========================================================================================================
        return: function will not return any thing
        """
        try:
            files = os.listdir(directory_path)
            n_row = 0

            for file in files:

                # skip all files except csv
                if not file.endswith(".csv"):
                    continue
                file_path = os.path.join(directory_path, file)
                # reading csv file using spark session
                # df = self.spark_session.read.csv(file_path)
                df = self.spark_session.read.csv(file_path,header=True,inferSchema=True)
                # sending dataframe to kafka topic iteratively
                for row in df.rdd.toLocalIterator():
                    message=",".join(map(str, list(row)))
                    print(message)
                    self.kafka_producer.send(self.kafka_topic_name,message)
                    n_row += 1
                    time.sleep(1)


                #df.foreach(lambda row: self.kafka_producer.send(self.kafka_topic_name, ",".join(map(str, list(row)))))
            return n_row
        except Exception as e:
            kafka_csv_data_producer_exp = KafkaCSVDataProducerException(
                "Error occurred  in module [{0}] class [{1}] method [{2}] ".
                    format(self.__module__, KafkaCSVDataProducer.__name__,
                           self.__init__.__name__))
            raise Exception(kafka_csv_data_producer_exp.error_message_detail(str(e), sys)) from e


"""
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 
"""
