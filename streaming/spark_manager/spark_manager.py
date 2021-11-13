import sys
import os
from pyspark.sql import SparkSession
from insurance_exception.insurance_exception import InsuranceException as SparkManagerException


class SparkManager:
    spark_session = None

    def __init__(self,app_name="ineuron-machine-learning"):
        """
        Creator:
        **********************************************************************************************************
        created date: 02 November 2021
        Organization: iNeuron
        author: avnish@ineuron.ai
        **********************************************************************************************************
        Description:
        **********************************************************************************************************
        SparkManager is responsible to return spark_session object.
        Any modification required should be done in SparkManager class
        """
        try:
            self.app_name=app_name
        except Exception as e:
            spark_manager_exception = SparkManagerException("Error occurred  in module [{0}] class [{1}] method [{2}] ".
                                                            format(self.__module__, SparkManager.__name__,
                                                                   self.__init__.__name__))
            raise Exception(spark_manager_exception.error_message_detail(str(e), sys)) from e

    def get_spark_session_object(self):
        """
        function will return spark session object
        """
        try:
            if SparkManager.spark_session is None:
                SparkManager.spark_session = SparkSession.builder.master("local").appName(self.app_name) \
                    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1")\
                    .config("spark.ui.port", "4041").getOrCreate()

            return SparkManager.spark_session
        except Exception as e:
            spark_manager_exception = SparkManagerException("Error occurred  in module [{0}] class [{1}] method [{2}] ".
                                                            format(self.__module__, SparkManager.__name__,
                                                                   self.get_spark_session_object.__name__))
            raise Exception(spark_manager_exception.error_message_detail(str(e), sys)) from e
