import pandas as pd
import numpy as np
import os
from pretty_html_table import build_table
from utils.common import delete_file
from application_logger import logging

class duplicate_index(object):
    """
    A coustom class that will give the indexes of the 
    duplicate questions that are filtered by the filter_duplicate
    module.

    Params: Filtered duplicated questions -> DataFrame, 
            question that needed to be checked for duplicate -> string
    
    Author: Biswajit Mohapatra, Swati Kahar

    Version: 1.0
    """

    def __init__(self,cleaned_nan_data,filtered_data,question):
        self.file_obj = open("duplicate_index.txt","a+")
        self.log = logging.App_Logger(self.file_obj)
        self.question = str(question)
        self.filtered_data = filtered_data
        self.cleaned_nan_data = cleaned_nan_data

    def index(self) -> list:
        """
        This method is responsible for getting the indexes
        of the duplicated questions for the given question.

        Params: NONE

        Author: Biswajit Mohapatra, Swati Kahar

        Version: 1.0
        """
        try:
            idx = list(np.where(self.question == self.filtered_data["clean_question_data"][::1])[0])
            return idx
        except KeyError as k:
            self.log.log(f"Something went wrong in index method :: {k}\n")
            raise KeyError
        except Exception as e:
            self.log.log(f"Something went wrong in index method :: {e}\n")
            raise Exception
    
    def details(self):
        """
        This method is responsible for giving a list of questions 
        that are duplicated.

        Author: Biswajit Mohapatra, Swati Kahar

        Version: 1.0
        """
        try:
            self.log.log(f"Entered the details method...\n")
            idx = self.index()
            data = self.filtered_data["index"].iloc[idx]
            data = self.cleaned_nan_data[["q_id","clean_question_data"]].iloc[data]
            template = "templates"
            file_name = "details.html"
            original_path = os.getcwd()
            path = os.path.join(template, file_name)
            if not os.path.exists(path):
                self.log.log(f"{file_name} isn't present so saving it...")
                os.chdir(template)
                html_table = build_table(data, 'blue_light')
                with open(file_name, 'w',encoding="utf-8") as f:
                    f.write(html_table)
                os.chdir(original_path)
                self.log.log(f"{file_name} saved successfully at {path}")
            else:
                self.log.log(f"{file_name} already exists so deleting it...")
                print(f"deleing dir {path}")
                self.log.log(f"creating new file at {path}...")
                os.chdir(template)
                delete_file(file_name)
                html_table = build_table(data, 'blue_light')
                with open(file_name, 'w',encoding="utf-8") as f:
                    f.write(html_table)
                os.chdir(original_path)
                self.log.log(f"New file created successfully at {path}")
            return 
        except KeyError as k:
            self.log.log(f"Something went wrong in idx_ques method :: {k}\n")
            raise KeyError
        except Exception as e:
            self.log.log(f"Something went wrong in idx_ques method :: {e}\n")
            raise Exception