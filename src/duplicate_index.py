import pandas as pd
import numpy as np
from application_logger import logging

class duplicate_index(object):
    """
    A coustom class that will give the indexes of the 
    duplicate questions that are filtered by the filter_duplicate
    module.

    Params: Filtered duplicated questions -> DataFrame, 
            question that needed to be checked for duplicate -> string
    
    Author: Biswajit Mohapatra

    Version: 1.0
    """

    def __init__(self,filtered_data,question):
        self.file_obj = open("duplicate_index.txt","a+")
        self.log = logging.App_Logger(self.file_obj)
        self.question = str(question)
        self.filtered_data = filtered_data

    def index(self) -> list:
        """
        This method is responsible for getting the indexes
        of the duplicated questions for the given question.

        Params: NONE

        Author: Biswajit Mohapatra

        Version: 1.0
        """
        try:
            return list(np.where(self.question == self.filtered_data["clean_question_data"][::1])[0])
        except KeyError as k:
            self.log.log(f"Something went wrong in index method :: {k}\n")
            raise KeyError
        except Exception as e:
            self.log.log(f"Something went wrong in index method :: {e}\n")
    
    def idx_ques(self):
        """
        This method is responsible for giving a list of questions 
        that are duplicated.

        Author: Biswajit Mohapatra

        Version: 1.0
        """
        try:
            return list(self.filtered_data["clean_question_data"].iloc[self.index])
        except KeyError as k:
            self.log.log(f"Something went wrong in idx_ques method :: {k}\n")
            raise KeyError
        except Exception as e:
            self.log.log(f"Something went wrong in idx_ques method :: {e}\n")
            raise Exception