import os
import sys

from utility import read_params, create_directory_path
from mongo_db.mongo_db_atlas import MongoDBOperation
import argparse
from utility import get_logger_object_of_training

from insurance_exception.insurance_exception import InsuranceException as GenericException


log_collection_name = "data_export"


class DataExporter:
    def __init__(self, config, logger, is_log_enable):
        try:
            self.config = config
            self.logger = logger
            self.is_log_enable = is_log_enable
            self.mongo_db = MongoDBOperation()
            self.dataset_database = self.config["dataset"]["database_detail"]["training_database_name"]
            self.dataset_collection_name = self.config["dataset"]["database_detail"]["dataset_training_collection_name"]
            self.training_file_from_db = self.config["artifacts"]['training_data']['training_file_from_db']
            self.master_csv = self.config["artifacts"]['training_data']['master_csv']
        except Exception as e:
            generic_exception = GenericException(
                "Error occurred in module [{0}] class [{1}] method [{2}]"
                    .format(self.__module__, DataExporter.__name__,
                            self.__init__.__name__))
            raise Exception(generic_exception.error_message_detail(str(e), sys)) from e

    def export_dataframe_from_database(self):
        try:
            create_directory_path(self.training_file_from_db)
            self.logger.log(f"Creating dataframe of data stored db"
                            f"[{self.dataset_database}] and collection[{self.dataset_collection_name}]")
            df = self.mongo_db.get_dataframe_of_collection(db_name=self.dataset_database,
                                                           collection_name=self.dataset_collection_name)
            master_csv_file_path = os.path.join(self.training_file_from_db, self.master_csv)
            self.logger.log(f"master csv file will be generated at "
                            f"{master_csv_file_path}.")
            df.to_csv(master_csv_file_path, index=None,header=True)

        except Exception as e:
            generic_exception = GenericException(
                "Error occurred in module [{0}] class [{1}] method [{2}]"
                    .format(self.__module__, DataExporter.__name__,
                            self.export_dataframe_from_database.__name__))
            raise Exception(generic_exception.error_message_detail(str(e), sys)) from e


def export_main(config_path: str, is_logging_enable=True,execution_id=None,executed_by=None) -> None:
    try:
        logger = get_logger_object_of_training(config_path=config_path, collection_name=log_collection_name,
                                               execution_id=execution_id, executed_by=executed_by)

        logger.is_log_enable = is_logging_enable
        config = read_params(config_path)
        data_exporter = DataExporter(config=config, logger=logger, is_log_enable=is_logging_enable)
        logger.log("Generating csv file from dataset stored in database.")
        data_exporter.export_dataframe_from_database()
        logger.log("Dataset has been successfully exported in directory and exiting export pipeline.")
    except Exception as e:
        generic_exception = GenericException(
            "Error occurred in module [{0}] method [{1}]"
                .format(export_main.__module__,
                        export_main.__name__))
        raise Exception(generic_exception.error_message_detail(str(e), sys)) from e


if __name__ == '__main__':
    args = argparse.ArgumentParser()
    args.add_argument("--config", default=os.path.join("config", "params.yaml"))
    parsed_args = args.parse_args()
    print("started")
    export_main(config_path=parsed_args.config)
