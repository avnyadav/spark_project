import importlib
import json
from datetime import datetime

import yaml
import uuid
import os
import shutil
from logger.logger import AppLogger

def get_time():
    """

    :return current time:
    """
    return datetime.now().strftime("%H:%M:%S").__str__()

def get_date():
    """

    :return current date:
    """
    return datetime.now().date().__str__()



def create_directory_path(path, is_recreate=True):
    """
    :param path:
    :param is_recreate: Default it will delete the existing directory yet you can pass
    it's value to false if you do not want to remove existing directory
    :return:
    """
    try:
        if is_recreate:
            if os.path.exists(path):
                shutil.rmtree(path, ignore_errors=False)  # remove existing directory if is_recreate is true
        os.makedirs(path, exist_ok=True)  # if directory is present it will not alter anything
        return True
    except Exception as e:
        raise e


def clean_data_source_dir(path, logger=None, is_logging_enable=True):
    try:
        if not os.path.exists(path):
            os.mkdir(path)
        for file in os.listdir(path):
            if '.gitignore' in file:
                pass
            logger.log(f"{os.path.join(path, file)}file will be deleted.")
            os.remove(os.path.join(path, file))
            logger.log(f"{os.path.join(path, file)}file has been deleted.")
    except Exception as e:
        raise e



def get_logger_object_of_training(config_path: str, collection_name, execution_id=None, executed_by=None) -> AppLogger:
    config = read_params(config_path)
    database_name = config['log_database']['training_database_name']
    if execution_id is None:
        execution_id = str(uuid.uuid4())
    if executed_by is None:
        executed_by = "Avnish Yadav"
    logger = AppLogger(project_id=5, log_database=database_name, log_collection_name=collection_name,
                       execution_id=execution_id, executed_by=executed_by)
    return logger


def get_logger_object_of_prediction(config_path: str, collection_name, execution_id=None,
                                    executed_by=None) -> AppLogger:
    config = read_params(config_path)
    database_name = config['log_database']['prediction_database_name']
    if execution_id is None:
        execution_id = str(uuid.uuid4())
    if executed_by is None:
        executed_by = "Avnish Yadav"
    logger = AppLogger(project_id=5, log_database=database_name, log_collection_name=collection_name,
                       execution_id=execution_id, executed_by=executed_by)
    return logger


def read_params(config_path: str) -> dict:
    with open(config_path) as yaml_file:
        config = yaml.safe_load(yaml_file)
    return config


def values_from_schema_function(schema_path):
    try:
        with open(schema_path, 'r') as r:
            dic = json.load(r)
            r.close()

        pattern = dic['SampleFileName']
        length_of_date_stamp_in_file = dic['LengthOfDateStampInFile']
        length_of_time_stamp_in_file = dic['LengthOfTimeStampInFile']
        column_names = dic['ColName']
        number_of_columns = dic['NumberofColumns']
        return pattern, length_of_date_stamp_in_file, length_of_time_stamp_in_file, column_names, number_of_columns
    except ValueError:
        raise ValueError

    except KeyError:
        raise KeyError

    except Exception as e:
        raise e

