import os
import signal

for i in range(10,20):
    print(i)
raise Exception
os.kill(os.getpid(), signal.SIGTERM)
print("child process",os.getpid(),"terminated")
