import os
import sys
import time


interval = sys.argv[1]
dir_name = os.path.dirname(os.path.abspath(__file__))
file_path = dir_name + "\\" + __file__
print("\n\n*****DO NOT KILL this program*****\n")
print("If you accidentally or intentionally killed this program, please rerun it.")
print("Rerun this program like this:\n", dir_name, ">python", file_path, interval)
print("This program runs processes every:", interval, "secs")
print("Program started at:", time.strftime("%c"))

while True:
    with open('log.txt', 'a') as fo:
        cur_time = time.strftime("%c")
        print("Current time:", cur_time, end="\r")
        fo.write(cur_time)

    time.sleep(int(interval))
