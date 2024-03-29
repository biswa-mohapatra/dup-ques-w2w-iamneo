"""
This is main function for the application.

Version : 2.0

Author: Biswajit Mohapatra
"""

# Importing all the dependencies...
import os
from flask.templating import render_template_string
import pandas as pd
import numpy as np
import pickle as pkl
from flask import Flask, request, render_template
from flask_cors import cross_origin
from sqlalchemy import null
from pretty_html_table import build_table
from src.question_duplicate_package import duplicate_v1
from src import data_prep,cloud_operations
from src.bulk_upload import bulk_upload_questions 
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

    Condition: The question that needed to be checked should be provided along with the school ID
    else the home page will be rendered by default.
    """
    if request.method == "POST":
        try:
            log.log(f"\n\n********************************Single question upload.*******************************************\n")
            log.log("Entered main function and getting the credentials from the config file.")
            scentence = request.form.get("Question")
            school_code = request.form.get("SchoolCode")
            scentence = scentence.strip().lower()
            config = read_yaml("config.yaml")
            local_dir = config["GET_DATA"]["local_dir"]
            auth_path = config["GET_DATA"]["auth_json_path"]
            transformed_data_file = config["GET_DATA"]["transformed_data_file"]
            school_code_list = config["GET_DATA"]["school_codes"]
            bucket_auth = config["GET_DATA"]["bucket_auth"]
            bucket_name = config["GET_DATA"]["bucket_name"]
            file_name = f"{school_code}_{transformed_data_file}"

            if not os.path.exists(file_name):# defining blob name for storing in cloud.
                blob_name = file_name 

                # file path to pass for cloud operations.
                upload_file_path = os.path.join(local_dir, file_name) 
                cloud_ops = cloud_operations.cloud_ops(bucket_auth,blob_name,bucket_name,upload_file_path)
                fileNames_recived = cloud_ops.list_blobs()
                
                # calling the question duplicate package.
                duplicate = duplicate_v1(auth_path)
                if not blob_name in fileNames_recived:
                    log.log(f"Blob with name :: {blob_name} isn't present, so fetching the data from Big-query.")
                    # defining path for transformed_data_file:

                    # Fetching the data
                    if not os.path.exists(local_dir):
                        log.log(log_message=f"creating data directory...")
                        create_directories([local_dir])
                    log.log(f"Connecting to the Big Query...")

                    # Connecting to Big Query
                    duplicate.connect_bigquerry() 
                    log.log(f"Connected to the Big Query...")
                    log.log(f"Getting the data...")

                    # Downloading the data from Big Query if data not present
                    if not os.path.exists(upload_file_path):
                        data =  duplicate.fetch_data(school_code=school_code,school_code_list=list(school_code_list)) 
                    else:
                        with open(upload_file_path, "rb") as f:
                            object = pkl.load(f)
                        data = pd.DataFrame(object) # reading the saved pickled data
                    log.log(f"Data getting completed...")

            
                    # transforming the cleaned data
                    if not os.path.exists(upload_file_path):
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
                        transformed_data = duplicate.transform_data(data_cleaned_nan)
                        log.log(f"Data tranformation completed, now saving the file to pickle.")

                        # saving the transformed data
                        transformed_data.to_pickle(upload_file_path) 
                        log.log(f"Data saved successfully at :: {upload_file_path}")

                        # uploading the saved data to google service.
                        log.log(f"Uploading the data to the google cloud storage.")
                        blob = cloud_ops.upload_file()
                        log.log(f"File uploaded successfully to the google cloude service with blob name as :: {blob.id}")

                        # Deleting the created file from local memory after uploading.
                        if os.path.exists(upload_file_path):
                            log.log(f"Deleting the file after uploading to the google storage :: {upload_file_path}")
                            delete_file(upload_file_path)
                            log.log(f"Deleted the file after uploading to the google storage :: {upload_file_path}")
                    else:
                        with open(upload_file_path, "rb") as f:
                            object = pkl.load(f)
                        transformed_data = pd.DataFrame(object)

                else:
                    log.log(f"Blob with {blob_name} present in the cloud storage, so staring the download.")
                    # downloading the file
                    cloud_ops.download_file(upload_file_path) 
                    log.log(f"Successfully downloaded the file.")
                    with open(upload_file_path, "rb") as f:
                            object = pkl.load(f)
                    transformed_data = pd.DataFrame(object)
            else:
                log.log(f"{file_name} already present in the cache storage.")
                with open(upload_file_path, "rb") as f:
                            object = pkl.load(f)
                transformed_data = pd.DataFrame(object)

            log.log(f"Starting Data Filteration \n")
            # Filtering out the data whose duplicates exists:
            filtered_data = duplicate.filter_duplicate(transformed_data)

            # deleting the file after downloading.
            #if os.path.exists(upload_file_path):
                #log.log(f"Data fetched successfully so deleting the downloaded file :: {upload_file_path}")
                #delete_file(upload_file_path)
                #log.log(f"Data fetched successfully so deleted the downloaded file :: {upload_file_path}")
            log.log(f"Data filteration completed...\n")

            #If the secentence is present, the following operation will be performed.
            if scentence:
                log.log(f"Finding duplicate index started...\n")
                idx = duplicate.find_dup_idx(filtered_data,scentence)
                template = "templates"
                file_name_html = "details.html"
                path = os.path.join(template, file_name_html)
                if len(idx)>1:
                    dup = duplicate.variations(filtered_data=filtered_data,idx=idx[1::])
                    new_data = duplicate.fetch_duplicate_data(dup)
                    original_path = os.getcwd()
                    if not os.path.exists(path):
                        log.log(f"{file_name_html} isn't present so saving it...")
                        os.chdir(template)
                        html_table = build_table(new_data, 'blue_light')
                        with open(file_name_html, 'w',encoding="utf-8") as f:
                            f.write(html_table)
                        os.chdir(original_path)
                        log.log(f"{file_name_html} saved successfully at {path}")
                    else:
                        log.log(f"creating new file at {path}...")
                        os.chdir(template)
                        delete_file(file_name_html)
                        html_table = build_table(new_data, 'blue_light')
                        with open(file_name_html, 'w',encoding="utf-8") as f:
                            f.write(html_table)
                        os.chdir(original_path)
                        log.log(f"New file created successfully at {path}")
                    return render_template('home.html',prediction_output = f"Number of Duplicates found :: {len(idx)-1}")
                else:
                    log.log(f"{file_name_html} already exists so deleting it...")
                    if os.path.exists(path):
                        delete_file(path)
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
        log.log(f"\n\n********************************Single upload page.*******************************************\n")
        log.log(f"Request method recived for details api...")
        path = "templates/details.html"
        if os.path.exists(path):
            log.log(f"File exists, hence rendering..")
            return render_template('details.html')
        else:
            log.log(f"File does not exist, hence rendering home page...")
            return render_template('home.html',output="No details present for 0 matches.")
    else:
        log.log(f"Request method not recived, hence rendering home.html")
        return render_template('home.html',output="No details present for 0 matches.")


@app.route("/bulk-upload", methods = ["GET", "POST"])
@cross_origin()
def bulk_upload():
    """
    This function is responsible for reciveing,processing and sending response to
    the UI.
    """
    if request.method == "POST":
        log.log(f"\n\n********************************Duplicate question check for bulk upload page.*******************************************\n")
        log.log(f"API for bulk-upload is being hitted successfully. Starting the bulk-upload process.")
        path = "templates/bulk_upload.html"
        if os.path.exists(path):
            log.log(f"{path} exists.")
            return render_template("bulk_upload.html")
        else:
            return render_template("home.html")

@app.route("/duplicate-question-bulk-upload", methods = ["GET", "POST"])
@cross_origin()
def question_details_bulk_upload():
    """
    This function is responsible for all the processing of the
    data recieved for the bulk upload.
    """
    if request.method == "POST":
        try:
            log.log(f"\n\n********************************Duplicate question check for bulk upload.*******************************************\n")
            question = request.form.get("BulkQuestion")
            school_code = request.form.get("SchoolCode")
            log.log("Sending the raw questions to prepare.")
            questions = data_prep.question_prepare(question)
            if type(question) != list:
                questions = []
                questions.append(question)
            log.log("Prepared the questions successfully.")
            config = read_yaml("config.yaml")
            local_dir = config["GET_DATA"]["local_dir"]
            auth_path = config["GET_DATA"]["auth_json_path"]
            transformed_data_file = config["GET_DATA"]["transformed_data_file"]
            school_code_list = config["GET_DATA"]["school_codes"]
            bucket_auth = config["GET_DATA"]["bucket_auth"]
            bucket_name = config["GET_DATA"]["bucket_name"]
            file_name = f"{school_code}_{transformed_data_file}"
            log.log(f"Checking for shcool code i.e., {school_code} presence.")
            file_path = os.path.join("data",file_name)
            
            if school_code in school_code_list:            #checking if the entered school code is a valid school code
                
                if not os.path.exists(file_path):
                    log.log(f"{file_name} doesn't exist in local cache memory.")
                    # defining blob name for storing in cloud.
                    blob_name = file_name

                    # file path to pass for cloud operations.
                    upload_file_path = os.path.join(local_dir, file_name)
                    log.log(f"Calling the cloud operation.") 
                    cloud_ops = cloud_operations.cloud_ops(bucket_auth,blob_name,bucket_name,upload_file_path)
                    log.log(f"Checking for {file_name} in cloud storage.")
                    fileNames_recived = cloud_ops.check_for_blob_presence()

                    # calling the question duplicate package.
                    duplicate = duplicate_v1(auth_path)
                    if not blob_name in fileNames_recived:
                        log.log(f"Blob with name :: {blob_name} isn't present, so fetching the data from Big-query.")
                        # defining path for transformed_data_file:

                        # Fetching the data
                        if not os.path.exists(local_dir):
                            log.log(log_message=f"creating data directory...")
                            create_directories([local_dir]) # creating a directory to store file if doesn't exist.
                        log.log(f"Connecting to the Big Query...")

                        # Connecting to Big Query
                        duplicate.connect_bigquerry() 
                        log.log(f"Connected to the Big Query...")
                        log.log(f"Getting the data...")

                        # Downloading the data from Big Query if data not present
                        if not os.path.exists(upload_file_path):
                            data =  duplicate.fetch_data(school_code=school_code,school_code_list=list(school_code_list)) 
                        else:
                            with open(upload_file_path, "rb") as f:
                                object = pkl.load(f)
                            data = pd.DataFrame(object) # reading the saved pickled data
                        log.log(f"Getting data completed...")


                        # transforming the cleaned data
                        if not os.path.exists(upload_file_path):
                            # Preparing the data:
                            log.log(f"{upload_file_path} doesn't exist, hence starting data preparation.")
                            prepared_data = data_prep.prepare_data(data)
                    
                            # Cleaning the data:
                            log.log("Instantiating cleaning of the data...")
                            data_cleaned = duplicate.clean_data(prepared_data)# removing the html tags from the data and storing the data
                            column_name = "clean_question_data"
                            data_cleaned_nan = duplicate.clean_nan(data_cleaned,column_name)# remove the nan created while removing the html tags
                            log.log(f"Data cleaning completed...")

                            #inserting column to the data:
                            col_name = "cleaned_mcq_questions_options"
                            duplicate.insert_col(data_cleaned_nan,18,col_name,value="")
                            log.log(f"{col_name} successfully added.")
                            transformed_data = duplicate.transform_data(data_cleaned_nan)
                            log.log(f"Data tranformation completed, now saving the file to pickle.")

                            # saving the transformed data
                            transformed_data.to_pickle(upload_file_path) 
                            log.log(f"Data saved successfully at :: {upload_file_path}")

                            # uploading the saved data to google service.
                            log.log(f"Uploading the data to the google cloud storage.")
                            blob = cloud_ops.upload_file()
                            log.log(f"File uploaded successfully to the google cloude service with blob name as :: {blob.id}")

                            # Deleting the created file from local memory after uploading.
                            if os.path.exists(upload_file_path):
                                log.log(f"Deleting the file after uploading to the google storage :: {upload_file_path}")
                                delete_file(upload_file_path)
                                log.log(f"Deleted the file after uploading to the google storage :: {upload_file_path}")
                        else:
                            log.log(f"{upload_file_path} exists, hence reading it.")
                            with open(upload_file_path, "rb") as f:
                                object = pkl.load(f)
                            transformed_data = pd.DataFrame(object)
                    else:
                        log.log(f"Blob with {blob_name} present in the cloud storage, so staring the download.")
                        # downloading the file
                        cloud_ops.download_file(upload_file_path) 
                        log.log(f"Successfully downloaded the file.")
                        with open(upload_file_path, "rb") as f:
                                object = pkl.load(f)
                        transformed_data = pd.DataFrame(object)
                else:
                    log.log(f"{file_name} is available at the local cache memory.")
                    with open(file_path, "rb") as f:
                            object = pkl.load(f)
                    transformed_data = pd.DataFrame(object)

                # Staring to check for duplicates for bulk uploaded questions.
                log.log(f"Checking for bulk-question duplicates.")
                if type(questions) != list:
                    log.log("Questions are not in list format")
                    questions = list(questions) # converting the list of questions into array
                bulkupload = bulk_upload_questions(questions,transformed_data)
                filtered_question = bulkupload.duplicate_questions()
                # Saving the filtred duplicate question table as html to render.
                if type(filtered_question) != str:
                    template = "templates"
                    file_name_html = "bulk-upload-dulicates.html"
                    path = os.path.join(template, file_name_html) # path to the html template.

                    original_path = os.getcwd()
                    if not os.path.exists(path):
                        log.log(f"{file_name_html} isn't present so saving it...")
                        os.chdir(template)
                        html_table = build_table(filtered_question, 'blue_light')
                        with open(file_name_html, 'w',encoding="utf-8") as f:
                            f.write(html_table)
                        os.chdir(original_path)
                        log.log(f"{file_name_html} saved successfully at {path}")
                    else:
                        log.log(f"creating new file at {path}...")
                        os.chdir(template)
                        delete_file(file_name_html)
                        html_table = build_table(filtered_question, 'blue_light')
                        with open(file_name_html, 'w',encoding="utf-8") as f:
                            f.write(html_table)
                        os.chdir(original_path)
                        log.log(f"New file created successfully at {path}")
                    return render_template("bulk-upload-dulicates.html")
                else:
                    return render_template("bulk_upload.html",result="No match found.")
            else:
                log.log(f"{school_code} is not available.")
                return render_template("bulk_upload.html",result="No match found for entered school code.")       
        except Exception as e:
            log.log(f"Something went wrong :: {e}")
            raise e

@app.route("/refresh", methods = ["GET", "POST"])
@cross_origin()
def refresh():
    """
    This function is responsible for deleting all the cache files in the GCS.
    """
    if request.method == "POST":
        try:
            log.log(f"\n\n********************************\tRefresh\t*******************************************\n")
            config = read_yaml("config.yaml")
            bucket_auth = config["GET_DATA"]["bucket_auth"]
            bucket_name = config["GET_DATA"]["bucket_name"]

            # Cheking the local cache memory:
            log.log(f"Clearing cache from local cache memory.")
            PATH = "data/"
            if os.path.exists(PATH):
                if len(os.listdir(PATH)) > 0:
                    files = os.listdir(PATH)
                    for file in files:
                        file_path = os.path.join(PATH,file)
                        log.log(f"{file} deleted from local directory.")
                        delete_file(file_path)
            log.log(f"Cleared all cache from local cache memory.")

            # Clearing all cache from cloud storage:
            log.log(f"Starting collection of blobs.")
            cloud_ops = cloud_operations.cloud_ops(bucket_auth=bucket_auth,blob_name=None,bucket_name=bucket_name,file_path=None)
            blob_list = cloud_ops.list_blobs()
            if len(blob_list) > 0:
                log.log(f"Collected all the blobs.")
                for blob_name in blob_list:
                    log.log(f"Deleting blob :: {blob_name}")
                    cloud_ops.delete_cloud_cache(bucket_name, blob_name)
                    log.log(f"Deleted blob :: {blob_name}")
                log.log(f"All the blobs from  bucket :: {bucket_name} are deleted.")

                return render_template("home.html",refresh_output = "All old records are deleted.")
            else:
                return render_template("home.html",refresh_output = "All old records are deleted.")
        except Exception as e:
            log.log(f"Error while refreshing the data :: {e}")
            raise Exception
                

if __name__ == "__main__":
    app.run(host='0.0.0.0',port=5000)