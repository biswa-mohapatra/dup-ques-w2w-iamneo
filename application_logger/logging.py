from datetime import datetime


class App_Logger:
    def __init__(self,file_object):
        self.file_object = file_object
        pass

    def log(self,log_message):
        self.now = datetime.now()
        self.date = self.now.date()
        self.current_time = self.now.strftime("%H:%M:%S")
        self.file_object.write(str(self.date) + "/" + str(self.current_time) + "\t\t" + log_message + "\n" )