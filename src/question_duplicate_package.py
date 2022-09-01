"""
THis is a custom module to perform every operation that could be required 
for finding the question duplicates.

"""
# Importng all the dependencies... 
from src.utils.common import read_yaml,create_directories,save_json,delete_file
from application_logger.logging import App_Logger
from pretty_html_table import build_table
from google.cloud import bigquery
from xmlrpc import client
import os, os.path
import shutil
from tqdm import tqdm
import pandas_gbq 
import pandas as pd
import numpy as np
import db_dtypes
import time
import re

STAGE = "QUESTION_DUPLICATE_PACKAGE"

class duplicate_v1(object):
    """
        This is a coustom class to perform all the necessary operations 
        required for filtering a duplicate.

        Params: Authentication file path : str

        Author: Biswajit Mohapatra

        Version: 3.0
        """    
    def __init__(self,auth_path):
        self.file_obj = open("questiion_duplicate_package.txt","a+")
        self.log = App_Logger(self.file_obj)
        self.j_file_path = auth_path
        self.cleaner = re.compile('<.*?>|&([a-z0-9]+|#[0-9]{1,6}|#x[0-9a-f]{1,6});')
        
    def connect_bigquerry(self):
        """
        This is a coustom defination for estrablishing connection
        with the bigquerry server.

        Params: None

        Author: Biswajit Mohapatra

        Version: 3.0
        """ 
        try:
            start_time = time.time()
            self.log.log(f"Connecting the bigquerry...")
            os.environ['GOOGLE_APPLICATION_CREDENTIALS']=os.path.expanduser(self.j_file_path)
            self.log.log(f"Connected to the bigquerry...\nTime taken :: {time.time() - start_time}")
        except Exception as e:
            self.log.log(f"Error connecting to the bigquerry :: {e}")
            raise Exception
                
        
    def fetch_data(self,school_code,school_code_list):
        """
        This is a coustom defination for extracting data from
        the bigquerry server.

        Params: Query string

        Author: Biswajit Mohapatra

        Version: 3.0
        """
        try:
            start_time = time.time()
            self.log.log(f"Fetching data from the bigquerry...")
            school_code = str(school_code)
            if school_code in school_code_list:
                query = f"""
                    WITH question_bank_schools as (
                    select qbbd.qb_id, qbbd.school_id, s.school_code from `examly_warehouse_mysql_replica.question_bank_branch_department` qbbd
                    left join `examly_warehouse_mysql_replica.schools` s
                    on s.school_id = qbbd.school_id
                    where qbbd.share = 0 and qbbd.deletedAt is null
                    group by qbbd.qb_id, qbbd.school_id, s.school_code
                    )

                    select
                    q.qb_id,
                    q.q_id,
                    q.question_data,
                    q.manual_difficulty,
                    q.question_type,
                    q.imported,
                    q.subject_id,
                    q.topic_id,
                    q.sub_topic_id,
                    (select name from `examly_warehouse_mysql_replica.subject` s where subject_id = q.subject_id group by name) as subject,
                    (select name from `examly_warehouse_mysql_replica.topic` where topic_id = q.topic_id group by name) as topic,
                    (select name from `examly_warehouse_mysql_replica.sub_topic` where sub_topic_id = q.sub_topic_id group by name) as subtopic,
                    qbs.school_id,
                    qbs.school_code,
                    q.createdAt as q_createdAt,
                    q.updatedAt as q_updatedAt,
                    q.deletedAt as q_deletedAt,
                    qb.createdAt as qb_createdAt,
                    qb.updatedAt as qb_updatedAt,
                    qb.deletedAt as qb_deletedAt,
                    mcq.options as mcq_questions_options,
                    mcq.answer as mcq_questions_answer,
                    qb.qb_name,
                    from `examly_warehouse_mysql_replica.questions` q
                    inner join `examly_warehouse_mysql_replica.question_banks` qb
                    on qb.qb_id = q.qb_id
                    inner join question_bank_schools qbs
                    on qbs.qb_id = qb.qb_id
                    and qbs.school_code IN ("{school_code}")
                    left join `examly_warehouse_mysql_replica.mcq_questions` mcq
                    on mcq.q_id = q.q_id
                    """
            else:
                query = f"""
                    WITH question_bank_schools as (
                    select qbbd.qb_id, qbbd.school_id, s.school_code from `examly_warehouse_mysql_replica.question_bank_branch_department` qbbd
                    left join `examly_warehouse_mysql_replica.schools` s
                    on s.school_id = qbbd.school_id
                    where qbbd.share = 0 and qbbd.deletedAt is null
                    group by qbbd.qb_id, qbbd.school_id, s.school_code
                    )

                    select
                    q.qb_id,
                    q.q_id,
                    q.question_data,
                    q.manual_difficulty,
                    q.question_type,
                    q.imported,
                    q.subject_id,
                    q.topic_id,
                    q.sub_topic_id,
                    (select name from `examly_warehouse_mysql_replica.subject` s where subject_id = q.subject_id group by name) as subject,
                    (select name from `examly_warehouse_mysql_replica.topic` where topic_id = q.topic_id group by name) as topic,
                    (select name from `examly_warehouse_mysql_replica.sub_topic` where sub_topic_id = q.sub_topic_id group by name) as subtopic,
                    qbs.school_id,
                    qbs.school_code,
                    q.createdAt as q_createdAt,
                    q.updatedAt as q_updatedAt,
                    q.deletedAt as q_deletedAt,
                    qb.createdAt as qb_createdAt,
                    qb.updatedAt as qb_updatedAt,
                    qb.deletedAt as qb_deletedAt,
                    mcq.options as mcq_questions_options,
                    mcq.answer as mcq_questions_answer,
                    qb.qb_name,
                    from `examly_warehouse_mysql_replica.questions` q
                    inner join `examly_warehouse_mysql_replica.question_banks` qb
                    on qb.qb_id = q.qb_id
                    inner join question_bank_schools qbs
                    on qbs.qb_id = qb.qb_id
                    and qbs.school_code IN ("neowise")
                    left join `examly_warehouse_mysql_replica.mcq_questions` mcq
                    on mcq.q_id = q.q_id
                    """
            # Fetching the whole data
            client = bigquery.Client()
            whole_data = pandas_gbq.read_gbq(query, use_bqstorage_api=True,dialect="standard")
            self.log.log(f"Data from bigquery downloaded...\nTime taken :: {time.time() - start_time}")
            return whole_data
        
        except ConnectionError as c:
            self.log.log(f"Error connecting to the bigquerry :: {c}")
            raise ConnectionError
        except Exception as e:
            self.log.log(f"Error connecting to the bigquerry :: {e}")
            raise Exception
       
    def col_to_drop(self,columns:list):
        """
        This is a coustom to drop desired columns.

        Params: List of column names.

        Author: Biswajit Mohapatra

        Version: 3.0
        """
        return self.data.drop(columns,axis=1)
    
    def select_data(self,whole_data,q_type:str):
        """
        This is a coustom defination to select a desiered question
        type from the entire data.

        Params: Data, Question type as string

        Author: Biswajit Mohapatra

        Version: 3.0
        """
        return whole_data.iloc[np.where(whole_data["question_type"] == q_type)]
    
    def insert_col(self,data,pos,col_name,value):
        """
        This is a coustom defination to select a desiered question
        type from the entire data.

        Params: Data
                Position -> At which position you want your coloumn to be.
                         -> Accepts only integers
                Coloumn Name -> What name you want for your inserted column will be specified here.
                Value -> The value you want your column should hold.

        Author: Biswajit Mohapatra

        Version: 3.0
        """
        return data.insert(pos,col_name,value)
    
    
    def clean_data(self,data):
        
        """
        This is a coustom defination for cleaning the question data i.e.,
        to remove the html contents.

        Params: Data

        Author: Biswajit Mohapatra

        Version: 3.0

        """
        try:
            self.log.log(f"Cleaning of the data started...")
            start_time = time.time()
            data["question_data"] = data["question_data"].replace(r"<p></p>",np.NaN,regex=True)
            if not "clean_question_data" in data.columns:
                data.insert(loc=3,column="clean_question_data",value=data["question_data"].str.replace(r'<[^<>]*>','',regex=True).replace('&amp','').replace('&nbsp','').replace('}',''))
                data["clean_question_data"] = data["clean_question_data"].map(lambda x: re.sub("&nbsp","",str(x)))
            else:
                data["clean_question_data"] = data["question_data"].str.replace(r'<[^<>]*>','',regex=True).replace('&amp','').replace('&nbsp','').replace('}','')
                data["clean_question_data"] = data["clean_question_data"].map(lambda x: re.sub("&nbsp","",str(x)))
            self.log.log(f"Data cleaned...\nTime taken :: {time.time() - start_time}")
            return data
        except KeyError as k:
            self.log.log(f"Something went wrong at clesning the data :: {k}")
            raise KeyError
        except IndexError as i:
            self.log.log(f"Something went wrong at clesning the data :: {i}")
            raise IndexError
        except Exception as e:
            self.log.log(f"Something went wrong at clesning the data :: {e}")
            raise Exception
    
    def cleanhtml(self,raw_html):
        
        """
        This function is responsible for cleaning extensive html tags from
        the answer data.

        Params: Raw Html data.

        Author: Biswajit Mohapatra

        Version: 3.0

        """
        try:
            cleantext = re.sub(self.cleaner, '', raw_html)
            return cleantext
        except Exception as e:
            self.log.log(f"Something went wrong in cleaning HTML tags from asnwers.")
            raise Exception
    
    def clean_answer_data(self,data,col_name:str):
        
        """
        This function is responsible for cleaning the answer data.

        Params: Data, Column Name : String

        Author: Biswajit Mohapatra

        Version: 3.0

        """
        try:
            start_time = time.time()
            if col_name in list(data.columns):
                none_idx = []
                self.log.log(f"{col_name} Column present....")
                for i in range(len(data["mcq_questions_options"])):
                    sample = data["mcq_questions_options"].iloc[i]
                    if not sample == None:
                        size = len(eval(list([sample])[0]))
                        data_ans = []
                        for n in range(size):
                            data_ans.append(self.cleanhtml(list(eval(list([sample])[0])[n].values())[0]))
                        data[col_name].iloc[i] = data_ans
                    else:
                        none_idx.append(i)
                self.log.log(f"Answer Data cleaned...\nTime taken :: {time.time() - start_time}")
                return data
            else:
                self.log.log(f"Insert cleaned_mcq_questions_options into your data...")
        except KeyError as k:
            self.log.log(f"Something went wrong at clesning the data :: {k}")
            raise KeyError
        except IndexError as i:
            self.log.log(f"Something went wrong at clesning the data :: {i}")
            raise IndexError
        except Exception as e:
            self.log.log(f"Something went wrong while cleaning the answer data. :: {e}")
            raise Exception
    
    
    def clean_nan(self,data,col_name:str):
        """
        This function is responsible for cleaning NaN values from given data and column.

        Params: Data, col_name : The coloumn from which you want the cleaning to be happened.

        Author: Biswajit Mohapatra

        Version: 3.0

        """
        try:
            self.log.log(f"Cleaning the NaN values from the data started....")
            data = data.drop(index=list(np.where(data[col_name] == "nan")[0]))
            self.log.log(f"Cleaning the NaN values from the data ended....")
            return data
        except KeyError as k:
            self.log.log(f"Something went wrong at clesning the NaN data :: {k}")
            raise KeyError
        except IndexError as i:
            self.log.log(f"Something went wrong at clesning the NaN data :: {i}")
            raise IndexError
        except Exception as e:
            self.log.log(f"Something went wrong while cleaning the NaN data. :: {e}")
            raise Exception
    
    def transform_data(self,cleaned_data):
        """
        This function is responsible for transforming the data.

        Params: Data : Cleaned data.

        Author: Biswajit Mohapatra

        Version: 3.0

        """
        try:
            self.log.log(f"Data transformation started...")
            start_time = time.time()
            data_new = cleaned_data
            data_new["clean_question_data"] = [str(data_new["clean_question_data"].iloc[i]).strip().lower() for i in range(len(data_new["clean_question_data"]))]
            self.log.log(f"Data transformation ended\nTime taken :: {time.time() - start_time}")
            return data_new
        except KeyError as k:
            self.log.log(f"Something went wrong at clesning the NaN data :: {k}")
            raise KeyError
        except IndexError as i:
            self.log.log(f"Something went wrong at clesning the NaN data :: {i}")
            raise IndexError
        except Exception as e:
            self.log.log(f"Something went wrong while cleaning the NaN data. :: {e}")
            raise Exception
    
    
    def filter_duplicate(self,transformed_data):
        """
        This function is responsible for filtering out the duplicate questions. 

        Params: Data : Transformed data.

        Author: Biswajit Mohapatra

        Version: 3.0

        """
        try:
            self.log.log(f"Started Filtering the duplicates... ")
            start_time = time.time()
            data = transformed_data
            data["duplicate"] = data["clean_question_data"].duplicated()
            duplicated = data.iloc[np.where(data["duplicate"] == True)]
            self.log.log(f"Ended Filtering the duplicates...\nTime taken :: {time.time() - start_time}")
            return duplicated.reset_index()
        except KeyError as k:
            self.log.log(f"Something went wrong at clesning the NaN data :: {k}")
            raise KeyError
        except IndexError as i:
            self.log.log(f"Something went wrong at clesning the NaN data :: {i}")
            raise IndexError
        except Exception as e:
            self.log.log(f"Something went wrong while cleaning the NaN data. :: {e}")
            raise Exception
    
    def find_dup_idx(self,data,scentence:str) -> list:
        """
        This function is responsible for finding the index of the duplicates of the given question.

        Params: Data : Filtered data. , Scentence : Question

        Author: Biswajit Mohapatra

        Version: 3.0

        """
        return list(np.where(scentence == data["clean_question_data"][::1])[0])
    
    
    # to show the variations:
    def variations(self,filtered_data,idx:list):
        """
        This function is responsible for showing the variations of the question whose duplicate exists.

        Params: Data : Filtered data. idx : Index of the duplicated data.

        Author: Biswajit Mohapatra

        Version: 3.0

        """
        try:
            self.log.log(f"Started Applying filtration for validation of duplicate data")
            start_time = time.time()
            if len(filtered_data.iloc[idx])>1:
                q_type = int(len(filtered_data["question_type"].iloc[idx]))
                if q_type > 1:
                    self.log.log(f"Ended Applying filtration for validation of duplicate data...\nVariation found...\nTime taken :: {time.time() - start_time}")
                    return filtered_data.iloc[idx]
            else:
                print(f"No variations found...\nTime taken :: {time.time() - start_time}")
        except KeyError as k:
            self.log.log(f"Something went wrong at clesning the NaN data :: {k}")
            raise KeyError
        except IndexError as i:
            self.log.log(f"Something went wrong at clesning the NaN data :: {i}")
            raise IndexError
        except Exception as e:
            self.log.log(f"Something went wrong while cleaning the NaN data. :: {e}")
            raise Exception

    # Fetching the data for which Answers are duplicate:
    def fetch_duplicate_data(self,data):
        """
        This function is responsible for fetching the duplicates whose answes are also similar.

        Params: Data : Cleaned answer data.

        Author: Biswajit Mohapatra

        Version: 3.0

        """
        try:
            self.log.log(f"Started fetching the duplicate data.")
            ans_cleaned_data3 = data 
            self.log.log(f"Ended fetching the duplicate data.")
            final_data = self.clean_answer_data(data,"cleaned_mcq_questions_options")
            final_data = ans_cleaned_data3.iloc[np.where(ans_cleaned_data3["duplicate"] == True)]
            return final_data[["index","qb_id","q_id","clean_question_data","manual_difficulty","question_type","imported","subject","topic","subtopic","school_code","q_createdAt","q_updatedAt","q_deletedAt","cleaned_mcq_questions_options","qb_createdAt","qb_updatedAt","qb_deletedAt","qb_name","duplicate"]]
        except KeyError as k:
            self.log.log(f"Something went wrong at clesning the NaN data :: {k}")
            raise KeyError
        except IndexError as i:
            self.log.log(f"Something went wrong at clesning the NaN data :: {i}")
            raise IndexError
        except Exception as e:
            self.log.log(f"Something went wrong while cleaning the NaN data. :: {e}")
            raise Exception