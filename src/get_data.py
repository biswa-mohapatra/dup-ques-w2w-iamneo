from xmlrpc import client
from application_logger import logging
from google.cloud import bigquery
import argparse
import os, os.path
import shutil
from tqdm import tqdm
import pandas_gbq 
import pandas as pd
from utils.common import read_yaml,create_directories

STAGE = "GET_DATA"

class data_downloader(object):
    """
    Custom class to download data fro bigquerry
    and save it as .csv file.

    Params: school id, destination path 

    Author: Biswajit Mohapatra, Swati Kahar
    """
    def __init__(self,school_id:str,file_destination_path:str):
        self.school_id = school_id
        self.file_path = file_destination_path
        self.file_obj = open("get_data.txt","a+")
        self.logger = logging.App_Logger(self.file_obj)
        self.config = read_yaml("config.yaml")
        self.file_name = self.config["GET_DATA"]["data_file_name"]
        self.auth_json_path = self.config["GET_DATA"]["auth_json_path"] 

    def download(self):
        """
        This function is responsible for downloading
        the data from the bigquerry.

        Params: None

        Author: Biswajit Mohapatra, Swati Kahar

        Version: 1.0

        """
        try:
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.expanduser(self.auth_json_path)
            client = bigquery.Client()
            
            query = f"""SELECT
            q_id,question_data
            FROM `examly-events.schema_testing_views.questions_dup_view`
            WHERE school_id = "{self.school_id}"
            """
            if not os.path.exists(self.file_path):
                self.logger.log(f"File doesn't exist of school id : {self.school_id}\n")
                data = pandas_gbq.read_gbq(query, use_bqstorage_api=True,dialect="standard")
                file_path = os.path.join(self.file_path,self.school_id)
                data.to_csv(file_path + self.file_name)
                self.logger.log(f"Data downloaded and saved at {file_path}...\n")
                return data
            else:
                self.logger.log(f"File already exist with school id {self.school_id}...\n")
                file_path = os.path.join(self.file_path,self.school_id)
                data = pd.read_csv(file_path + self.file_name)
                return data
        except Exception as e:
            self.logger.log(f"Something went wrong in get_data method...{e}\n")
            raise e

    


