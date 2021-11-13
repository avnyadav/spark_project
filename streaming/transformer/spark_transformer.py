import pandas as pd

from insurance_exception.insurance_exception import InsuranceException as SparkTransformerException
import os, sys
from mongo_db.mongo_db_atlas import MongoDBOperation


class SparkTransformer():
    def __init__(self, database_name, collection_name):
        try:
            self.database_name = database_name
            self.collection_name = collection_name
            self.mongo_db = MongoDBOperation()
            self.ml_transformer = []


        except Exception as e:
            spark_transformer_exception = SparkTransformerException("Error occurred  in module [{0}] class [{1}] "
                                                                    "method [{2}] ".
                                                                    format(self.__module__, SparkTransformer.__name__,
                                                                           self.__init__.__name__))
            raise Exception(spark_transformer_exception.error_message_detail(str(e), sys)) from e

    def add_machine_learning_transformer(self, transformer: list):
        try:
            self.ml_transformer.extend(transformer)
        except Exception as e:
            spark_transformer_exception = SparkTransformerException("Error occurred  in module [{0}] class [{1}] "
                                                                    "method [{2}] ".
                                                                    format(self.__module__, SparkTransformer.__name__,
                                                                           self.__init__.__name__))
            raise Exception(spark_transformer_exception.error_message_detail(str(e), sys)) from e

    def process_each_record(self, dataframe,epoch_id):
        try:
            dataframe = dataframe.toPandas()
            if dataframe.shape[0] > 0:
                dataframe['timestamp'] = pd.to_datetime(dataframe['timestamp'])
                self.mongo_db.insert_dataframe_into_collection(db_name=self.database_name,
                                                               collection_name=self.collection_name,
                                                               data_frame=dataframe)
                dataframe.to_csv("new_data.csv", index=None)
        except Exception as e:
            spark_transformer_exception = SparkTransformerException("Error occurred  in module [{0}] class [{1}] "
                                                                    "method [{2}] ".
                                                                    format(self.__module__, SparkTransformer.__name__,
                                                                           self.process_each_record.__name__))
            raise Exception(spark_transformer_exception.error_message_detail(str(e), sys)) from e
