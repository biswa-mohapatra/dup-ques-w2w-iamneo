import os
import pandas as pd
import get_data
from utils.common import read_yaml,create_directories
from application_logger.logging import App_Logger

def main():
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

main()


    
