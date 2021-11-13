from streaming.producer.kafka_csv_data_producer import KafkaCSVDataProducer
from streaming.spark_manager.spark_manager import SparkManager

if __name__ == "__main__":
    try:
        path = "prediction_files"
        spark_session = SparkManager().get_spark_session_object()
        kfk_csv_data_producer = KafkaCSVDataProducer(
            spark_session=spark_session,

        )
        kfk_csv_data_producer.send_csv_data_to_kafka_topic(directory_path=path)
    except Exception as e:
        print(e)
