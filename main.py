import os
import pandas as pd
import get_data,data_clean,transform_data
from utils.common import read_yaml,create_directories
from application_logger.logging import App_Logger

def main():
    try:
        file_obj =open("MAIN.txt","a+")
        config = read_yaml("config.yaml")
        school_id = config["GET_DATA"]["school_id"]
        local_dir = config["GET_DATA"]["local_dir"]
        log = App_Logger(file_obj)
        
        if not os.path.exists(local_dir):
            log.log(log_message=f"creating data directory...")
            create_directories([local_dir])
        
        log.log(log_message=f"Getting the data...")
        data =  get_data.data_downloader(school_id,local_dir)
        data = data.download()
        log.log(log_message=f"Data getting completed...")

        log.log("Instantiating claning of the data...")
        data_cleaned = data_clean.clean_data(data)
        data_cleaned = data_cleaned.remove_tags(drop_null=True)
        PATH = os.path.join(local_dir,"cleaned_data.csv")
        if not os.path.exists(PATH):
            data_cleaned.to_csv(PATH)
        log.log(f"Data cleaning completed...")

    except KeyError as k:
        log.log(f"Something went wrong :: {k}\n")
        raise KeyError
    except ValueError as v:
        log.log(f"Something went wrong :: {v}\n")
        raise ValueError
    except Exception as e:
        log.log(f"Something went wrong :: {e}\n")
        raise Exception

main()


    
