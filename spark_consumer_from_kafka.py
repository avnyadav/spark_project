import os

from pyspark.ml import PipelineModel
from pyspark.ml.regression import RandomForestRegressionModel

from streaming.spark_manager.spark_manager import SparkManager
from streaming.consumer.kafka_to_spark_csv_consumer import KafkaToSparkCSVConsumer


if __name__ == "__main__":
    spark_session = SparkManager().get_spark_session_object()

    schema_string = "age INT,sex STRING,bmi DOUBLE,children INT,smoker STRING,region STRING"
    database_name = "stream_prediction"
    collection_name = "insurance_prediction_output"
    kfk_con = KafkaToSparkCSVConsumer(spark_session=spark_session,
                                      schema_string=schema_string,
                                      database_name=database_name,
                                      collection_name=collection_name
                                      )
    transformer_list = []
    pipeline_model = PipelineModel.load(os.path.join("artifacts",
                                                     "pipeline",
                                                     "pipeline_model"))
    random_forest_model = RandomForestRegressionModel.load(os.path.join("artifacts",
                                                                        "model",
                                                                        "random_forest_regressor"))

    transformer_list.append(pipeline_model)
    transformer_list.append(random_forest_model)
    kfk_con.spark_transformer.add_machine_learning_transformer(
        transformer=transformer_list
    )
    kfk_con.receive_csv_data_from_kafka_topics()
