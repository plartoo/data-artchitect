"""
TODO: update descriptions
Usage:
> python my_chron.py example_script_to_run.py 3600
to run 'example_script_to_run.py' every hour
"""

import os
import sys
import time
import subprocess

log_folder = "Logs/"
python_script = sys.argv[1]
interval = sys.argv[2]
dir_name = os.path.dirname(os.path.abspath(__file__))
file_path = dir_name + "\\" + __file__
print("\n\n*****DO NOT KILL this program*****\n")
print("If you accidentally or intentionally killed this program, please rerun it.")
print("Rerun this program like this:\n", dir_name, ">python", file_path, interval)
print("This program runs processes every:", interval, "secs")
print("Program started at:", time.strftime("%c"))

while True:
    log_file = log_folder + os.path.splitext(python_script)[0] + '.txt'
    with open(log_file, 'a') as fo:
        cur_time = time.strftime("%c")
        print("Current time:", cur_time, end="\r")
        fo.write(cur_time + "\n")

        try:
            cmd = ' '.join(['python', python_script])
            print(cmd, "\n")
            output = subprocess.check_output(cmd, stderr=subprocess.PIPE)
            fo.write(str(output) + "\n")
        except Exception as err:
            print(err)
            raise

    time.sleep(int(interval))
