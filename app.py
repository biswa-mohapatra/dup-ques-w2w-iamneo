import os
import pandas as pd
from flask import Flask, request, render_template
from flask_cors import cross_origin
from sqlalchemy import null
from src import get_data,data_clean,transform_data,filter_duplicate,duplicate_index
from utils.common import read_yaml,create_directories
from application_logger.logging import App_Logger

app = Flask(__name__)
file_obj =open("MAIN.txt","a+")
log = App_Logger(file_obj)

@app.route("/")
@cross_origin()
def home():
    return render_template("home.html")

@app.route("/duplicate", methods = ["GET", "POST"])
@cross_origin()
def main():
    if request.method == "POST":
        try:
            print("Entered main function...")
            scentence = request.form.get("Question")
            print(scentence)
            config = read_yaml("config.yaml")
            school_id = config["GET_DATA"]["school_id"]
            local_dir = config["GET_DATA"]["local_dir"]
            
            
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

            filtered_data_PATH = os.path.join(local_dir,f"filtered_data{school_id}.csv")
            if not os.path.exists(filtered_data_PATH):
                log.log(f"Starting Data Filteration \n")
                f = filter_duplicate.filter(transformed_data)
                filtered_data = f.filter_duplicates()
                log.log(f"Data filteration completed...\n")
                filtered_data.to_csv(filtered_data_PATH)
                log.log(f"Data filteration completed and filtered data saved at {filtered_data_PATH}...\n")
            else:
                filtered_data = pd.read_csv(filtered_data_PATH)


            if scentence:
                print(scentence)
                log.log(f"Finding duplicate index started...\n")
                dup = duplicate_index.duplicate_index(cleaned_nan_data=data_cleaned,filtered_data=filtered_data,question=str(scentence))
                dup_list = dup.index()
                print(len(dup_list))
                if len(dup_list)>1:
                    dup.details()
                    return render_template('home.html',prediction_output = f"Number of Duplicates found :: {len(dup_list)-1} \n school_id :: {school_id}")
                else:
                    dup.details()
                    return render_template('home.html',prediction_output = f"Number of Duplicates found :: {len(dup_list)} \n school_id :: {school_id}")


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

    else:
        return render_template('home.html',prediction_output = "No values were added...")

@app.route("/details", methods=["GET", "POST"])
@cross_origin()
def details():
    if request.method == "POST":
        path = "templates/details.html"
        if os.path.exists(path):
            return render_template('details.html')
        else:
            return render_template('home.html')
    else:
        return render_template('home.html')

if __name__ == "__main__":
    #result = main("What is your name?")
    #print(result)
    #main()
    app.run(debug=True)
