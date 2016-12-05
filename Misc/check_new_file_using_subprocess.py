"""
A way to check list of dirs and files in S3 bucket
"""
import subprocess

bucket = 's3://diap.prod.us-east-1.target/'
folder = 'FacebookDatamart/'

output = subprocess.check_output(['aws','s3','ls',bucket+folder])
ls_of_objs = [s.strip() for s in output.splitlines()]

