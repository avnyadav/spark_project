
import random
import sys

import os
import argparse

from pyspark.sql.types import IntegerType, StringType, FloatType
from sklearn.metrics import r2_score, mean_squared_error

from utility import create_directory_path,read_params
import numpy as np
from utility import get_logger_object_of_training
from pyspark.ml import Pipeline
from pyspark.ml.regression import  RandomForestRegressor
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler

from insurance_exception.insurance_exception import InsuranceException as GenericException

from streaming.spark_manager.spark_manager import SparkManager

log_collection_name = "training_model"


class DataPreProcessing:
    def __init__(self, logger, is_log_enable=True, data_frame=None, pipeline_path=None):
        try:
            self.logger = logger
            self.logger.is_log_enable = is_log_enable
            self.data_frame = data_frame
            self.stages = []
            self.pipeline_path = pipeline_path
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

    def encode_categorical_column(self, input_columns: list):
        try:
            string_indexer = StringIndexer(inputCols=input_columns,
                                           outputCols=[f"{column}_encoder" for column in input_columns])
            self.stages.append(string_indexer)
            one_hot_encoder = OneHotEncoder(inputCols=string_indexer.getOutputCols(),
                                            outputCols=[f"{column}_encoded" for column in input_columns])
            self.stages.append(one_hot_encoder)

        except Exception as e:
            generic_exception = GenericException(
                "Error occurred in module [{0}] class [{1}] method [{2}]"
                    .format(self.__module__, DataPreProcessing.__name__,
                            self.encode_categorical_column.__name__))
            raise Exception(generic_exception.error_message_detail(str(e), sys)) from e

    def create_input_features(self, required_column: list):
        """

        """
        try:
            vector_assembler = VectorAssembler(inputCols=required_column, outputCol="input_features")
            self.stages.append(vector_assembler)
        except Exception as e:
            generic_exception = GenericException(
                "Error occurred in module [{0}] class [{1}] method [{2}]"
                    .format(self.__module__, DataPreProcessing.__name__,
                            self.create_input_features.__name__))
            raise Exception(generic_exception.error_message_detail(str(e), sys)) from e

    def get_train_test_dataframe(self, test_size=0.2):
        try:
            train_df, test_df = self.data_frame.randomSplit([1 - test_size, test_size], seed=random.randint(0, 1000))
            self.logger.log(f"Training dataset count {train_df.count()}")
            self.logger.log(f"Test dataset count {test_df.count()}")
            return train_df, test_df
        except Exception as e:
            generic_exception = GenericException(
                "Error occurred in module [{0}] class [{1}] method [{2}]"
                    .format(self.__module__, DataPreProcessing.__name__,
                            self.get_train_test_dataframe.__name__))
            raise Exception(generic_exception.error_message_detail(str(e), sys)) from e

    def get_prepared_dataset(self, ):
        try:
            schema_definition = {"age": IntegerType(),
                                 "sex": StringType(),
                                 "bmi": FloatType(),
                                 "children": IntegerType(),
                                 "smoker": StringType(),
                                 "expenses": FloatType()
                                 }
            self.update_dataframe_scheme(schema_definition=schema_definition)
            self.encode_categorical_column(input_columns=["sex", "smoker"])
            required_column = ['age', 'bmi', 'children', 'sex_encoded', 'smoker_encoded', ]
            self.create_input_features(required_column=required_column)
            pipeline = Pipeline(stages=self.stages)
            pipeline_fitted_obj = pipeline.fit(self.data_frame)
            self.data_frame = pipeline_fitted_obj.transform(self.data_frame)
            # os.remove(path=self.pipeline_path)
            create_directory_path(self.pipeline_path, is_recreate=True)
            pipeline_fitted_obj.write().overwrite().save(self.pipeline_path)
            return self.get_train_test_dataframe(test_size=0.2)
        except Exception as e:
            generic_exception = GenericException(
                "Error occurred in module [{0}] class [{1}] method [{2}]"
                    .format(self.__module__, DataPreProcessing.__name__,
                            self.get_prepared_dataset.__name__))
            raise Exception(generic_exception.error_message_detail(str(e), sys)) from e


class ModelTrainer:

    def __init__(self, config, logger, is_log_enable):
        try:
            self.logger = logger
            self.logger.is_log_enable = is_log_enable
            self.config = config
            self.training_file_path = self.config['artifacts']['training_data']['training_file_from_db']
            self.master_csv = self.config['artifacts']['training_data']['master_csv']
            self.target_columns = self.config['target_columns']['columns']
            self.test_size = self.config['base']['test_size']
            self.random_state = self.config['base']['random_state']
            self.plot = self.config['artifacts']['training_data']['plots']
            self.pipeline_path = self.config['artifacts']['training_data']['pipeline_path']
            self.model_path = config['artifacts']['model']['model_path']
            self.null_value_file_path = config['artifacts']['training_data']['null_value_info_file_path']
            """
            self.spark = SparkSession.builder.\
                master("local[*]").\
                appName("insurance-premium-reg").getOrCreate()
                """
            self.spark = SparkManager().get_spark_session_object()
            """
            self.spark=SparkSession.builder.appName('app_name') \
                .master('local[*]') \
                .config('spark.sql.execution.arrow.pyspark.enabled', True) \
                .config('spark.sql.session.timeZone', 'UTC') \
                .config('spark.driver.memory', '32G') \
                .config('spark.ui.showConsoleProgress', True) \
                .config('spark.sql.repl.eagerEval.enabled', True) \
                .getOrCreate()
                """
        except Exception as e:
            generic_exception = GenericException(
                "Error occurred in module [{0}] class [{1}] method [{2}]"
                    .format(self.__module__, ModelTrainer.__name__,
                            self.__init__.__name__))
            raise Exception(generic_exception.error_message_detail(str(e), sys)) from e


    def save_regression_metric_data(self, y_true, y_pred, title):
        try:
            y_true = np.array(y_true).reshape(-1)
            y_pred = np.array(y_pred).reshape(-1)
            rmse = np.sqrt(mean_squared_error(y_true, y_pred))
            r_squared_score = r2_score(y_true, y_pred)
            msg = f"{title} R squared score: {r_squared_score:.3%}"
            self.logger.log(msg)
            print(msg)
            msg = f"{title} Root mean squared error: {rmse:.3}"
            self.logger.log(msg)
            print(msg)
        except Exception as e:
            generic_exception = GenericException(
                "Error occurred in module [{0}] class [{1}] method [{2}]"
                    .format(self.__module__, ModelTrainer.__name__,
                            self.save_regression_metric_data.__name__))
            raise Exception(generic_exception.error_message_detail(str(e), sys)) from e

    def get_dataframe(self):
        try:
            master_file_path = os.path.join(self.training_file_path, self.master_csv)

            return self.spark.read.csv(master_file_path, header=True, inferSchema=True)
        except Exception as e:
            generic_exception = GenericException(
                "Error occurred in module [{0}] class [{1}] method [{2}]"
                    .format(self.__module__, ModelTrainer.__name__,
                            self.get_dataframe.__name__))
            raise Exception(generic_exception.error_message_detail(str(e), sys)) from e

    def data_preparation(self):
        try:
            data_frame = self.get_dataframe()
            preprocessing = DataPreProcessing(logger=self.logger,
                                              is_log_enable=self.logger.is_log_enable,
                                              data_frame=data_frame,
                                              pipeline_path=self.pipeline_path)
            return preprocessing.get_prepared_dataset()
        except Exception as e:
            generic_exception = GenericException(
                "Error occurred in module [{0}] class [{1}] method [{2}]"
                    .format(self.__module__, ModelTrainer.__name__,
                            self.data_preparation.__name__))
            raise Exception(generic_exception.error_message_detail(str(e), sys)) from e

    def begin_training(self):
        try:
            train_df, test_df = self.data_preparation()
            random_forest_regressor = RandomForestRegressor(featuresCol="input_features", labelCol="expenses")
            random_forest_model = random_forest_regressor.fit(train_df)
            train_prediction = random_forest_model.transform(train_df)
            testing_prediction = random_forest_model.transform(test_df)
            training_data = train_prediction.select("expenses", "prediction").toPandas()
            testing_data = testing_prediction.select("expenses", "prediction").toPandas()
            self.save_regression_metric_data(training_data['expenses'], training_data['prediction'],
                                             title="Training score")
            self.save_regression_metric_data(testing_data['expenses'], testing_data['prediction'],
                                             title="Testing score")

            self.save_model(model=random_forest_model, model_name="random_forest_regressor")
            self.spark.stop()
        except Exception as e:
            generic_exception = GenericException(
                "Error occurred in module [{0}] class [{1}] method [{2}]"
                    .format(self.__module__, ModelTrainer.__name__,
                            self.begin_training.__name__))
            raise Exception(generic_exception.error_message_detail(str(e), sys)) from e

    def save_model(self, model, model_name, intermediate_path=None):
        try:

            if intermediate_path is None:
                model_path = os.path.join(self.model_path)
            else:
                model_path = os.path.join(self.model_path, intermediate_path)
            create_directory_path(model_path, )
            model_full_path = os.path.join(model_path, f"{model_name}")
            self.logger.log(f"Saving mode: {model_name} at path {model_full_path}")
            # os.remove(path=model_full_path)
            model.write().overwrite().save(model_full_path)

        except Exception as e:
            generic_exception = GenericException(
                "Error occurred in module [{0}] class [{1}] method [{2}]"
                    .format(self.__module__, ModelTrainer.__name__,
                            self.save_model.__name__))
            raise Exception(generic_exception.error_message_detail(str(e), sys)) from e


def train_main(config_path: str, datasource: str, is_logging_enable=True, execution_id=None, executed_by=None) -> None:
    try:
        logger = get_logger_object_of_training(config_path=config_path, collection_name=log_collection_name,
                                               execution_id=execution_id, executed_by=executed_by)

        logger.is_log_enable = is_logging_enable
        logger.log("Training begin.")
        config = read_params(config_path)
        model_trainer = ModelTrainer(config=config, logger=logger, is_log_enable=is_logging_enable)
        model_trainer.begin_training()
        logger.log("Training completed successfully.")

    except Exception as e:
        generic_exception = GenericException(
            "Error occurred in module [{0}] method [{1}]"
                .format(train_main.__module__,
                        train_main.__name__))
        raise Exception(generic_exception.error_message_detail(str(e), sys)) from e


if __name__ == '__main__':
    args = argparse.ArgumentParser()
    args.add_argument("--config", default=os.path.join("config", "params.yaml"))
    args.add_argument("--datasource", default=None)
    parsed_args = args.parse_args()
    print(parsed_args.config)
    print(parsed_args.datasource)
    train_main(config_path=parsed_args.config, datasource=parsed_args.datasource)
