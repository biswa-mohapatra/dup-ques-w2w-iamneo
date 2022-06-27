import pandas as pd
import numpy as np
from application_logger import logging

class filter(object):
    """
    A coustom class to filter the existing duplicates 
    in the in the given school data.

    Params: Transformed data

    Author: Biswajit Mohapatra, Swati Kahar

    Version: 1.0
    """
    def __init__(self,transformed_data):
        self.file_obj = open("filter_data.txt","a+")
        self.log = logging.App_Logger(self.file_obj)
        self.data = transformed_data

    def filter_duplicates(self):
        """
        This function is resposible for filtering the duplicates
        in the given data.

        Author: Biswajit Mohapatra, Swati Kahar
        """

        try:
            self.log.log(f"Started filtering duplicates...\n")
            data = self.data
            data["duplicate"] = data["clean_question_data"].duplicated()
            duplicated = data.iloc[np.where(data["duplicate"] == True)]
            self.log.log(f"Finished filtering duplicates...\n")
            return duplicated.reset_index()

        except KeyError as k:
            self.log.log(f"Something went wrong in filter_duplicates method :: {k}\n")
            raise KeyError
        except TypeError as t:
            self.log.log(f"Something went wrong in filter_duplicates method :: {t}\n")
            raise TypeError
        except Exception as e:
            self.log.log(f"Something went in filter_duplicates method :: {e}\n")
            raise Exception