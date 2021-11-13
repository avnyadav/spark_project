import os
from datetime import datetime
import uuid
import sys
from insurance_exception.insurance_exception import InsuranceException as AppLoggerException
from mongo_db.mongo_db_atlas import MongoDBOperation


class AppLogger:
    def __init__(self, project_id, log_database, log_collection_name, executed_by,
                 execution_id, is_log_enable=True):
        try:

            self.log_database = log_database
            self.log_collection_name = log_collection_name
            self.executed_by = executed_by
            self.execution_id = execution_id
            self.mongo_db_object = MongoDBOperation()
            self.project_id = project_id
            self.is_log_enable = is_log_enable
        except Exception as e:
            app_logger_exception = AppLoggerException(
                "Error occurred in module [{0}] class [{1}] method [{2}]"
                    .format(MongoDBOperation.__module__.__str__(), MongoDBOperation.__name__,
                            "__init__"))
            raise Exception(app_logger_exception.error_message_detail(str(e), sys)) from e

    def log(self, log_message):
        try:
            if not self.is_log_enable:
                return 0
            log_data = {
                'execution_id': self.execution_id,
                'message': log_message,
                'executed_by': self.executed_by,
                'project_id': self.project_id,
                'updated_date_and_time': datetime.now().strftime("%H:%M:%S")
            }

            self.mongo_db_object.insert_record_in_collection(
                self.log_database, self.log_collection_name, log_data)
        except Exception as e:
            app_logger_exception = AppLoggerException(
                "Error occurred in module [{0}] class [{1}] method [{2}]"
                    .format(MongoDBOperation.__module__.__str__(), MongoDBOperation.__name__,
                            self.log.__name__))
            raise Exception(app_logger_exception.error_message_detail(str(e), sys)) from e
