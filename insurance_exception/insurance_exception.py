import os


class InsuranceException(Exception):

    def __init__(self, error_message):
        """

        :param error_message: error message in string format
        """
        self.error_message = error_message

    def __repr__(self):
        return InsuranceException.__name__.__str__()

    def error_message_detail(self, error, error_detail):
        exc_type, exc_obj, exc_tb = error_detail.exc_info()
        file_name = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        error_message = "python script name [{0}] line number [{1}] error message [{2}]".format(file_name,
                                                                                                exc_tb.tb_lineno,
                                                                                                str(error))
        self.error_message = self.error_message + " " + error_message
        return self.error_message

    def __str__(self):
        return self.error_message
