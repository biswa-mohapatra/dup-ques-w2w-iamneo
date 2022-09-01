# Importing the necessary dependencies...
import time
import pandas as pd
import numpy as np
from application_logger.logging import App_Logger



class bulk_upload_questions(object):
    """
    This class is responsible for performing all the necessary
    operations that are necessary to find duplicate questions
    in the questions that are bulk uploaded.

    Params: questions : array of questions
            transformed data : pd.DataFrame
    Returns: details of the duplicate questions : pd.DataFrame

    Exceptions:

    Author: Biswajit Mohapatra
    """
    def __init__(self, questions,transformed_data):
        self.questions = questions
        self.transformed_data = transformed_data
        self.file_obj = open("bulk_upload.txt","a+")
        self.log = App_Logger(self.file_obj)
    
    def duplicate_questions(self):
        """
        Will find the common questions from historical data to the bulk_question_data.
        Params: None
        Returns: common questions : pd.DataFrame

        Exceptions: 

        Author: Biswajit Mohapatra
        """
        try:
            self.log.log(f"Starting the question duplicate check for bulk upload questions.")
            start_time = time.time()
            #self.questions = list(self.questions)
            #print(self.questions)
            questions_dataframe = pd.DataFrame(data=self.questions,index=range(len(self.questions)),columns=["Questions"])
            common_questions = pd.merge(left=self.transformed_data["clean_question_data"],right=questions_dataframe,how='inner',left_on="clean_question_data",right_on="Questions",copy=False)
            if common_questions.shape[0] > 0:
                self.log.log(f"Merging of the questions with whole data completed.\nTime taken :: {time.time()-start_time}")
                data = {
                        "Question_data":questions_dataframe["Questions"],
                        "Is_Duplicate_Questions":common_questions["Questions"].unique()
                        }
                filter_data = pd.DataFrame(data = np.nan,index=range(len(data["Question_data"])),columns=list(data.keys()))
                filter_data["Question_data"] = pd.Series(data["Question_data"])
                filter_data["Is_Duplicate_Questions"] = pd.Series(data["Is_Duplicate_Questions"])
                for i in range(len(filter_data["Is_Duplicate_Questions"])):
                    if type(filter_data["Is_Duplicate_Questions"][i]) == str:
                        filter_data["Is_Duplicate_Questions"][i] = "Yes"
                    else:
                        filter_data["Is_Duplicate_Questions"][i] = "No"
                self.log.log(f"""Finding question duplicates completed.\nTime taken :: {time.time() - start_time}""")
                return filter_data
            else:
                return "No match found"
        except ValueError as v:
            self.log.log(f"Some values are missing :: {v}")
            raise ValueError
        except Exception as e:
            self.log.log(f"Something went wrong :: {e}")
            raise Exception
