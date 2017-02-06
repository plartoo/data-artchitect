import os


class Logger:
    def __init__(self, script_file_name, folder_name='Logs'):
        self.script_file_name = script_file_name
        self.folder_name = folder_name
        # we can do '.log', but for now, just let it be
        self.log_file_name = os.path.splitext(self.script_file_name)[0] + '.txt'
        self.log_file_with_path = os.path.join(folder_name, self.log_file_name)

        if not os.path.exists(folder_name):
            os.makedirs(folder_name)

    def log(self, msg):
        with open(self.log_file_with_path, 'a') as fo:
            fo.write(msg + "\n")


