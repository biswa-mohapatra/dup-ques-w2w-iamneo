"""
This is a coustoum library that will help us uploading, downloading
and perform storing of the files in the Google cloud storage.

Author: Biswajit Mohapatra

Version: 2.0
"""

# Importing all the dependencies...
import os
from google.cloud import storage
from application_logger.logging import App_Logger

class cloud_ops(object):
    """
    This is a coustom library for performing cloud operations.

    Params: Path for bucket authentication credentials -> str
            Blob name -> str : Name of the file to be uploaded.
            Bucket name -> str : Name of the bucket in the Google cloud storage.
            File path -> str : Path to the file to be uploaded.
    Returns: None

    Exceptions:

    Author: Biswajit Mohapatra

    Version: 2.0
    """
    def __init__(self,bucket_auth,blob_name,bucket_name,file_path):
        self.file_obj = open("cloud_operations.txt","a+")
        self.log = App_Logger(self.file_obj)
        self.bucket_auth = bucket_auth
        self.bucket_name = bucket_name
        self.file_path = file_path
        self.blob_name = blob_name

    
    def connect_to_storage(self):
        """
        This function will connect to the cloud storage service using the provided credentials.

        Params: None
        Returns: True on connection.

        Exceptions: Connection error if credentials are not provided.

        Author: Biswajit Mohapatra

        Version: 2.0
        """
        try:
            # setting up the enviroment
            self.log.log(f"Connecting to the cloud storage service using credentials.")
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = self.bucket_auth
            self.log.log(f"Connected to the cloud storage service using credentials.")
            return True
        except ConnectionError as c:
            self.log.log(f"Something went wrong while connecting to the cloud storage service :: {c}")

    
    def check_for_blob_presence(self):
        """
        This function will check if the particluar blob is already present in the cloud storage
        or not.

        Params: None
        Returns : True if the blob is present in the cloud storage

        Exceptions:

        Author: Biswajit Mohapatra

        Version: 2.0
        """
        try:
            # connecting to the cloud storage service
            self.log.log(f"Connecting to cloud storage service")
            self.connect_to_storage()
            self.log.log(f"Connected to cloud storage service")

            # Calling the client object method
            storage_client = storage.Client()
            bucket_name = str(storage_client.get_bucket(self.bucket_name)).replace('<',"").replace("Bucket:","").replace(">","").strip()
            bucket_list = []
            for bucket in storage_client.list_buckets(max_results=226):
                bucket_list.append(str(bucket).replace('<',"").replace("Bucket:","").replace(">","").strip())

            if bucket_name in bucket_list:
                self.log.log(f"Bucket {bucket_name} already exists..")
                file_list = []
                for i in bucket.client.list_blobs(bucket_name):
                    file_list.append(str(i).split(",")[1].strip())
                return file_list
        except ConnectionError as c:
            self.log.log(f"Something went wrong while trying to connect :: {c}")
        except Exception as e:
            self.log.log(f"Something went wrong while checking for bucket presence :: {e}")

    def upload_file(self):
        """
        This function is responsible for uploading a file to the bucket specified.

        Params: None
        Returns: Uploaded message.

        Exceptions:

        Author: Biswajit Mohapatra

        Version: 2.0
        """
        try:
            # connecting to the cloud storage service
            self.log.log(f"Connecting to cloud storage service")
            self.connect_to_storage()
            self.log.log(f"Connected to cloud storage service")

            # Calling the client object method
            storage_client = storage.Client()
            self.log.log("Entered the upload file method.")
            bucket = storage_client.get_bucket(self.bucket_name)
            blob = bucket.blob(self.blob_name)
            # Uploading the file
            blob.upload_from_filename(self.file_path)
            return blob
        except ConnectionError as c:
            self.log.log(f"Something went wrong in estrablishing a connection :: {c}")
        except Exception as e:
            self.log.log(f"Something went wrong in uploading file :: {e}")        
    
    def download_file(self,download_path:str):
        """
        This is a function to download the file from the google cloud storage
        by taking the file name.

        Params: filename: The name of the file -> str
        Returns: path to the downloaded file -> str
        
        Exceptions: 

        Author: Biswajit Mohapatra

        Version: 2.0
        """

        try:
            # connectiong to the google cloud storage
            self.log.log(f"Connecting to cloud storage service")
            self.connect_to_storage()
            self.log.log(f"Connected to cloud storage service")

            # Calling the client object method
            storage_client = storage.Client()
            bucket = storage_client.get_bucket(self.bucket_name)
            blob = bucket.blob(self.blob_name)
            if self.blob_name.split(".")[1] == "pkl":
                file_path = download_path
            else:
                file_path = download_path + self.blob_name + ".pkl"
            with open(file_path, 'wb') as f:
                storage_client.download_blob_to_file(blob, f)
            self.log.log(f'Downloaded and Saved the file at :: {download_path}')
        except ConnectionError as c:
            self.log.log(f"Something went wrong in estrablishing a connection :: {c}")
        except Exception as e:
            self.log.log(f"Something went wrong in downloading the file :: {e}")
    

        