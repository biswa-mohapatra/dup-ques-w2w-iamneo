import os
import pandas as pd
from src import get_data,data_clean,transform_data,filter_duplicate,duplicate_index
from utils.common import read_yaml,create_directories
from application_logger.logging import App_Logger

def main(question:str)->str:
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

        PATH = os.path.join(local_dir,f"cleaned_data{school_id}.csv")
        if not os.path.exists(PATH):
            log.log("Instantiating claning of the data...")
            data_cleaned = data_clean.clean_data(data)
            data_cleaned = data_cleaned.remove_tags(drop_null=True,PATH=PATH)
            data_cleaned.to_csv(PATH)
            log.log(f"Data cleaning completed...")
        else:
            data_cleaned = pd.read_csv(PATH)
        transform_data_PATH = os.path.join(local_dir,f"transformed_data{school_id}.csv")
        if not os.path.exists(transform_data_PATH):
            log.log(f"Starting Data Transformation \n")
            t = transform_data.transform(data_cleaned)
            transformed_data = t.transform_data()
            log.log(f"Data transformation completed...\n")
            transformed_data.to_csv(transform_data_PATH)
            log.log(f"Transformed data saved at {transform_data_PATH}")
        else:
            transformed_data = pd.read_csv(transform_data_PATH)

        log.log(f"Starting Data Filteration \n")
        f = filter_duplicate.filter(transformed_data)
        filtered_data = f.filter_duplicates()
        log.log(f"Data filteration completed...\n")

        if question:
            log.log(f"Finding duplicate index started...\n")
            dup = duplicate_index.duplicate_index(filtered_data,str(question))
            dup = dup.index()
            if len(dup)>1:
                return f"Number of Duplicates found :: {len(dup)-1}"
            else:
                return f"Number of duplicates found :: {len(dup)}"


    except KeyError as k:
        log.log(f"Something went wrong :: {k}\n")
        raise KeyError
    except AttributeError as a:
        log.log(f"Something went wrong :: {a}\n")
        raise AttributeError
    except ValueError as v:
        log.log(f"Something went wrong :: {v}\n")
        raise ValueError
    except Exception as e:
        log.log(f"Something went wrong :: {e}\n")
        raise Exception

if __name__ == "__main__":
    result = main("What is your name?")
    print(result)
    #main()

    
