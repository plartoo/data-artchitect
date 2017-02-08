import os
import time


class Logger:
    def __init__(self, script_file_name, log_folder_name='Logs'):
        self.script_file_name = script_file_name
        self.log_folder_name = log_folder_name
        # we can do '.log', but for now, just let it be
        self.log_file_name = os.path.splitext(self.script_file_name)[0] + '.txt'
        self.log_file_with_path = os.path.join(self.log_folder_name, self.log_file_name)

        if not os.path.exists(log_folder_name):
            os.makedirs(log_folder_name)

    def log(self, log_msg):
        with open(self.log_file_with_path, 'a') as fo:
            fo.write(log_msg + "\n")


    def log_time_taken(self, start_ctime, end_ctime):
        log_msg = "\n\n" + self.script_file_name + "\n"
        log_msg += "was last invoked at: " + start_ctime + "\n"
        log_msg += "finished running at: " + end_ctime + "\n"
        pattern = '%a %b %d %H:%M:%S %Y'
        time_taken = time.mktime(time.strptime(end_ctime, pattern)) - time.mktime(time.strptime(start_ctime, pattern))
        log_msg += "Total time taken for this iteration (secs): " + str(time_taken)

        with open(self.log_file_with_path, 'a') as fo:
            fo.write(log_msg + "\n")
