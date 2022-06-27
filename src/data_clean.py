import os
import shutil
import pandas as pd 
import numpy as np
import regex
from application_logger import logging

class clean_data(object):
    """
    This is a coustom class to clean the downloaded data.

    Params: data -> DataFrame

    Author: Biswajit Mohapatra, Swati Kahar

    Version: 1.0 
    """

    def __init__(self, data):
        self.data = data
        self.file_obj = open("clean_data.txt","a+") 
        self.log = logging.App_Logger(self.file_obj)
    
    def cols_to_drop(self,columns_to_drop:list):
        """
        This function is responsible for dropping the passed columns.

        Params: Columns -> list

        Author: Biswajit Mahapatra, Swati Kahar

        Version: 1.0 
        """
        try:
            self.log.log(f"Dropping columns :: {columns_to_drop}")
            data = self.data.drop(columns_to_drop,axis=1)
            self.log.log(f"Successfully dropped columns :: {columns_to_drop}")
            return data
        except KeyError as k:
            self.log.log(f"Something went wrong in cols_to_drop method :: {k}\n")
            raise KeyError
        except Exception as e:
            self.log.log(f"Something went wrong in cols_to_drop method, during dropping columns :: {columns_to_drop}\n \t ERROR :: {e}\n")
            raise e

    def remove_tags(self,drop_null:bool,PATH):
        """
        This is a coustom function to remove 
        exsting html tags and other functional tags.

        Params: Columns -> list

        Author: Biswajit Mahapatra, Swati Kahar

        Version: 1.0
        """
        try:
            if not os.path.exists(PATH):
                self.log.log(f"Cleaning of tags from the data started....\n")
                self.data = self.data.replace(r"<p></p>",np.NaN,regex=True)
                if drop_null:
                    self.data = self.data[::1].dropna()

                # Removing the html tags:
                self.data["clean_question_data"] = self.data["question_data"].str.replace(r'<[^<>]*>','',regex=True)
                self.log.log(f"Cleaning of tags from the data completed....\n")
                self.data = self.data.drop(index=list(np.where(self.data["clean_question_data"] == "nan")[0]))
                self.log.log(f"NaN values wrere removed successfully....\n")
                return self.data
        except KeyError as k:
            self.log.log(f"Something went wrong in remove_tags method... :: {k}\n")
            raise KeyError
        except Exception as e:
            self.log.log(f"Something went wrong in remove_tags method :: {e}\n")
            raise e



        