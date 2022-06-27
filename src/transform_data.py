import pandas as pd 
import numpy as np 
from application_logger import logging 

class transform(object):
    """
    A coustom class that transforms the data to lower case and 
    then removes the unwanted spaces.

    Params: Cleaned data 

    Author: Biswajit Mohapatra, Swati Kahar

    Version: 1.0  
    """

    def __init__(self,cleaned_data):
        self.data = cleaned_data
        self.file_obj = open("transform_data.txt","a+")
        self.log = logging.App_Logger(self.file_obj)

    def transform_data(self):
        """
        To tranform the given data to lower case and
        remove the unwanted spaces.

        Author: Biswajit Mahapatra, Swati Kahar
        """
        try:
            self.log.log(f"Transforming the data started...")
            data_new = self.data
            data_new["clean_question_data"] = [str(data_new["clean_question_data"].iloc[i]).strip().lower() for i in range(len(data_new["clean_question_data"]))]
            self.log.log(f"Data Transformation successful...")
            return data_new
        except KeyError as k:
            self.log.log(f"Something went wrong in transform_data method :: {k}\n")
            raise KeyError
        except Exception as e:
            self.log.log(f"Something went wrong in transform_data method :: {e}\n")

        