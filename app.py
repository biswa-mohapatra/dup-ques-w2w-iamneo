import os
import pandas as pd
from flask import Flask, request, render_template
from flask_cors import cross_origin
from sqlalchemy import null
from pretty_html_table import build_table
from src.question_duplicate_package import duplicate_v1
from src import data_prep 
from src.utils.common import read_yaml,create_directories,delete_file
from application_logger.logging import App_Logger

app = Flask(__name__)
file_obj =open("MAIN.txt","a+")
log = App_Logger(file_obj)

@app.route("/")
@cross_origin()
def home():
    """
    This function is responsible for rendering the home page.
    """
    return render_template("home.html")

@app.route("/duplicate", methods = ["GET", "POST"])
@cross_origin()
def main():
    """
    This function is responsible for rendering the number of duplicate
    along with schoolID.

    Condition: The question that needed to be checked should be provided
    else the home page will be rendered by default.
    """
    if request.method == "POST":
        try:
            print("Entered main function...")
            scentence = request.form.get("Question")
            print(scentence)
            config = read_yaml("config.yaml")
            school_id = config["GET_DATA"]["school_id"]
            local_dir = config["GET_DATA"]["local_dir"]
            query = config["GET_DATA"]["query"]
            auth_path = config["GET_DATA"]["auth_json_path"]
            duplicate = duplicate_v1(auth_path)
            
            if not os.path.exists(local_dir):
                log.log(log_message=f"creating data directory...")
                create_directories([local_dir])
            log.log(f"Connecting to the Big Query...")
            duplicate.connect_bigquerry() # Connecting to Big Query
            log.log(f"Connected to the Big Query...")
            log.log(f"Getting the data...")
            data =  duplicate.fetch_data(query=query) # Downloading the data from Big Query
            log.log(f"Data getting completed...")

            # Preparing the data:
            prepared_data = data_prep.prepare_data(data)
        
        # Cleaning the data:
            log.log("Instantiating claning of the data...")
            data_cleaned = duplicate.clean_data(prepared_data)# removing the html tags from the data and storing the data
            column_name = "clean_question_data"
            data_cleaned_nan = duplicate.clean_nan(data_cleaned,column_name)# remove the nan created while removing the html tags
            log.log(f"Data cleaning completed...")

            #inserting column to the data:
            col_name = "cleaned_mcq_questions_options"
            duplicate.insert_col(data_cleaned_nan,18,col_name,value="")
            log.log(f"{col_name} successfully added.")
        
            # transforming the cleaned data
            transformed_data = duplicate.transform_data(data_cleaned_nan)

            log.log(f"Starting Data Filteration \n")
            # Filtering out the data whose duplicates exists:
            filtered_data = duplicate.filter_duplicate(transformed_data)
            log.log(f"Data filteration completed...\n")
            scentence = data_cleaned_nan["clean_question_data"].iloc[100]
            if scentence:
                print(scentence)
                log.log(f"Finding duplicate index started...\n")
                idx = duplicate.find_dup_idx(filtered_data,scentence)
                print(len(idx))
                if len(idx)>1:
                    dup = duplicate.variations(filtered_data=filtered_data,idx=idx)
                    template = "templates"
                    file_name = "details.html"
                    new_data = duplicate.fetch_duplicate_data(dup)
                    original_path = os.getcwd()
                    path = os.path.join(template, file_name)
                    if not os.path.exists(path):
                        log.log(f"{file_name} isn't present so saving it...")
                        os.chdir(template)
                        final_data = duplicate.fetch_duplicate_data(dup)
                        html_table = build_table(new_data, 'blue_light')
                        with open(file_name, 'w',encoding="utf-8") as f:
                            f.write(html_table)
                        os.chdir(original_path)
                        log.log(f"{file_name} saved successfully at {path}")
                    else:
                        log.log(f"{file_name} already exists so deleting it...")
                        print(f"deleing dir {path}")
                        log.log(f"creating new file at {path}...")
                        os.chdir(template)
                        delete_file(file_name)
                        html_table = build_table(new_data, 'blue_light')
                        with open(file_name, 'w',encoding="utf-8") as f:
                            f.write(html_table)
                        os.chdir(original_path)
                        log.log(f"New file created successfully at {path}")
                    return render_template('home.html',prediction_output = f"Number of Duplicates found :: {len(idx)-1}")
                else:
                    return render_template('home.html',prediction_output = f"Number of Duplicates found :: 0")


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
    """
    This function is responsible for rendering the details of duplicate
    question on a different page.

    Condition: The file must be present before rendering
    else the home page will only be rendered. 
    """
    if request.method == "POST":
        log.log(f"Request method recived for details api...")
        path = "templates/details.html"
        if os.path.exists(path):
            log.log(f"File exists, hence rendering..")
            return render_template('details.html')
        else:
            log.log(f"File does not exist, hence rendering home page...")
            return render_template('home.html')
    else:
        log.log(f"Request method not recived, hence rendering home.html")
        return render_template('home.html')

if __name__ == "__main__":
    app.run(debug=True)