import sys
import os
import argparse

from pyspark.sql.types import IntegerType, FloatType, StringType

from utility import create_directory_path,get_logger_object_of_prediction,read_params

from streaming.spark_manager.spark_manager import SparkManager

from insurance_exception.insurance_exception import InsuranceException as GenericException
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.regression import RandomForestRegressionModel, RandomForestRegressor

log_collection_name = "prediction_model"


class DataPreProcessing:
    def __init__(self, logger, is_log_enable=True, data_frame=None, pipeline_path=None):
        try:
            self.logger = logger
            self.logger.is_log_enable = is_log_enable
            self.data_frame = data_frame
            print(pipeline_path)
            self.pipeline_obj = PipelineModel.load(pipeline_path)

        except Exception as e:
            generic_exception = GenericException(
                "Error occurred in module [{0}] class [{1}] method [{2}]"
                    .format(self.__module__, DataPreProcessing.__name__,
                            self.__init__.__name__))
            raise Exception(generic_exception.error_message_detail(str(e), sys)) from e

    def set_dataframe(self, dataframe):
        try:
            self.data_frame = dataframe
        except Exception as e:
            generic_exception = GenericException(
                "Error occurred in module [{0}] class [{1}] method [{2}]"
                    .format(self.__module__, DataPreProcessing.__name__,
                            self.update_dataframe_scheme.__name__))
            raise Exception(generic_exception.error_message_detail(str(e), sys)) from e

    def update_dataframe_scheme(self, schema_definition: dict):
        """

        """
        try:
            print(self.data_frame.printSchema())
            if self.data_frame is None:
                raise Exception("update the attribute dataframe")
            for column, datatype in schema_definition.items():
                self.logger.log(f"Update datatype of column: {column} to {str(datatype)}")
                self.data_frame = self.data_frame.withColumn(column, self.data_frame[column].cast(datatype))
        except Exception as e:
            generic_exception = GenericException(
                "Error occurred in module [{0}] class [{1}] method [{2}]"
                    .format(self.__module__, DataPreProcessing.__name__,
                            self.update_dataframe_scheme.__name__))
            raise Exception(generic_exception.error_message_detail(str(e), sys)) from e

    def get_prepared_data(self):
        try:
            schema_definition = {"age": IntegerType(),
                                 "sex": StringType(),
                                 "bmi": FloatType(),
                                 "children": IntegerType(),
                                 "smoker": StringType(),
                                 }
            self.update_dataframe_scheme(schema_definition=schema_definition)
            self.data_frame = self.pipeline_obj.transform(self.data_frame)
            print(self.data_frame.printSchema())
            return self.data_frame
        except Exception as e:
            generic_exception = GenericException(
                "Error occurred in module [{0}] class [{1}] method [{2}]"
                    .format(self.__module__, DataPreProcessing.__name__,
                            self.get_prepared_data.__name__))
            raise Exception(generic_exception.error_message_detail(str(e), sys)) from e


class Predictor:

    def __init__(self, config, logger, is_log_enable):
        try:
            self.logger = logger
            self.logger.is_log_enable = is_log_enable
            self.config = config
            self.prediction_file_path = self.config['artifacts']['prediction_data']['prediction_file_from_db']
            self.master_csv = self.config['artifacts']['prediction_data']['master_csv']
            self.model_path = self.config['artifacts']['model']['model_path']
            self.prediction_output_file_path = self.config['artifacts']['prediction_data'][
                'prediction_output_file_path']
            self.prediction_file_name = self.config['artifacts']['prediction_data']['prediction_file_name']
            self.target_columns = self.config['target_columns']['columns']
            self.null_value_file_path = config['artifacts']['training_data']['null_value_info_file_path']
            self.pipeline_path = self.config['artifacts']['training_data']['pipeline_path']
            """
            self.spark = SparkSession.builder. \
                master("local[*]"). \
                appName("insurance-premium-reg").getOrCreate()
                """
            self.spark = SparkManager().get_spark_session_object()
        except Exception as e:
            generic_exception = GenericException(
                "Error occurred in module [{0}] class [{1}] method [{2}]"
                    .format(self.__module__, Predictor.__name__,
                            self.__init__.__name__))
            raise Exception(generic_exception.error_message_detail(str(e), sys)) from e

    def get_dataframe(self):
        try:
            master_file_path = os.path.join(self.prediction_file_path, self.master_csv)
            return self.spark.read.csv(master_file_path, header=True, inferSchema=True)
        except Exception as e:
            generic_exception = GenericException(
                "Error occurred in module [{0}] class [{1}] method [{2}]"
                    .format(self.__module__, Predictor.__name__,
                            self.get_dataframe.__name__))
            raise Exception(generic_exception.error_message_detail(str(e), sys)) from e

    def data_preparation(self):
        try:

            input_features = self.get_dataframe()
            data_preprocess = DataPreProcessing(logger=self.logger,
                                                is_log_enable=self.logger.is_log_enable,
                                                data_frame=input_features,
                                                pipeline_path=self.pipeline_path
                                                )
            return data_preprocess.get_prepared_data()

        except Exception as e:
            generic_exception = GenericException(
                "Error occurred in module [{0}] class [{1}] method [{2}]"
                    .format(self.__module__, Predictor.__name__,
                            self.data_preparation.__name__))
            raise Exception(generic_exception.error_message_detail(str(e), sys)) from e


    def load_model(self):
        try:
            model_path = self.model_path
            if not os.path.exists(model_path):
                raise Exception(f"Model directory: {model_path} is not found.")
            model_names = os.listdir(model_path)
            if len(model_names) != 1:
                raise Exception(f"We have expected only one model instead we found {len(model_names)}")
            model_name = model_names[0]
            model_path = os.path.join(model_path, model_name)
            print(f"model path: {model_path}")
            return RandomForestRegressionModel.load(model_path)
        except Exception as e:
            generic_exception = GenericException(
                "Error occurred in module [{0}] class [{1}] method [{2}]"
                    .format(self.__module__, Predictor.__name__,
                            self.load_model.__name__))
            raise Exception(generic_exception.error_message_detail(str(e), sys)) from e

    def predict(self):
        try:

            input_data = self.data_preparation()
            model = self.load_model()
            print(str(model))
            print(input_data.printSchema())
            prediction = model.transform(input_data)
            prediction_output = prediction.select("age", "sex", "children", "smoker", "prediction").toPandas()
            create_directory_path(self.prediction_output_file_path)
            output_file_path = os.path.join(self.prediction_output_file_path, self.prediction_file_name)
            if prediction_output is not None:
                prediction_output.to_csv(output_file_path, index=None, header=True)
        except Exception as e:
            generic_exception = GenericException(
                "Error occurred in module [{0}] class [{1}] method [{2}]"
                    .format(self.__module__, Predictor.__name__,
                            self.predict.__name__))
            raise Exception(generic_exception.error_message_detail(str(e), sys)) from e


def predict_main(config_path: str, datasource: str, is_logging_enable=True, execution_id=None,
                 executed_by=None) -> None:
    try:
        logger = get_logger_object_of_prediction(config_path=config_path, collection_name=log_collection_name,
                                                 execution_id=execution_id, executed_by=executed_by)

        logger.is_log_enable = is_logging_enable
        logger.log("Prediction begin.")
        config = read_params(config_path)
        predictor = Predictor(config=config, logger=logger, is_log_enable=is_logging_enable)
        predictor.predict()
        logger.log("Prediction completed successfully.")

    except Exception as e:
        generic_exception = GenericException(
            "Error occurred in module [{0}] method [{1}]"
                .format(predict_main.__module__,
                        predict_main.__name__))
        raise Exception(generic_exception.error_message_detail(str(e), sys)) from e


if __name__ == '__main__':
    args = argparse.ArgumentParser()
    args.add_argument("--config", default=os.path.join("config", "params.yaml"))
    args.add_argument("--datasource", default=None)
    parsed_args = args.parse_args()
    print(parsed_args.config)
    print(parsed_args.datasource)
    predict_main(config_path=parsed_args.config, datasource=parsed_args.datasource)
