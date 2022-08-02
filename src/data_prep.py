"""
This is a custom module to perform every function for 
preparation of the data.
"""
# Importing all the dependencies...
from application_logger.logging import App_Logger
import pandas as pd
import numpy as np

def prepare_data(data):
    """
    A coustom function to prepare the data that will further fed for preprocessing.

    Params: raw data
    Returns: Prepared data

    Exceptions:

    Author: Biswajit Mohapatra

    Version: 2.0
    """

    try:
        file_obj =open("data_prep.txt","a+")
        log = App_Logger(file_obj)
        # filtering the not deleted data from the whole data:
        idx_create = set()
        for i in range(len(data["question_data"])):
            if data["q_deletedAt"].iloc[i] is not pd.NaT:
                idx_create.add(i)
            elif data["qb_deletedAt"].iloc[i] is not pd.NaT:
                idx_create.add(i)

        # Separating the not deleted data from whole data:
        whole_data_notDeleted = data.drop(data.iloc[list(idx_create)].index)

        # Finding Empty question data rows:

        print(f"Number of questions in actual data :: {whole_data_notDeleted.shape[0]}")
        log.log(f"Number of questions in actual data :: {whole_data_notDeleted.shape[0]}")
        no_ques_data = whole_data_notDeleted.iloc[np.where(whole_data_notDeleted['question_data'] == "<p></p>")]
        print(f"Number of rows containing empty question :: {no_ques_data.shape[0]}")
        log.log(f"Number of rows containing empty question :: {no_ques_data.shape[0]}")
         

        # Storing the data where question is present:
        return whole_data_notDeleted.drop(index=no_ques_data.index,axis=0)
    except KeyError as k:
        log.log(f"Something went wrong at clesning the data :: {k}")
        raise KeyError
    except IndexError as i:
        log.log(f"Something went wrong at clesning the data :: {i}")
        raise IndexError
    except Exception as e:
        log.log(f"Something went wrong at clesning the data :: {e}")
        raise Exception