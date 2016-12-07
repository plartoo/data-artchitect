import os
import subprocess

cmd = ' '.join(['python', 'testchild.py'])
print(cmd)
for i in range(10):
    output = subprocess.check_output(cmd)  # run(cmd)#check_output(cmd)
    print(i, output)
print("parent process",os.getpid(),"done")
