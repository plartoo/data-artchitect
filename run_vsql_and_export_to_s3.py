"""
Run VSQL via Python subprocess and export the result as CSV to S3.
Takes four input parameters like this:
> python run_vsql_and_export.py <sql_script_file.sql> <local_folder_to_store_output> <s3_folder_to_store_output> <file_prefix>
"""
import sys
import subprocess
import time

from s3_utils import *

try:
    date_and_time = time.strftime('%Y%m%d_%H%M%S')  # in YYYMMDD_HHMMSS format
    sql_script = sys.argv[1]        # e.g., "sqltest.sql"
    local_folder_name = sys.argv[2] # e.g., "Facebook/"
    s3_folder_name = sys.argv[3]    # e.g., "FilesForDatamart/Facebook/"
    out_file_name = sys.argv[4] + "_" + date_and_time + ".csv"
    output_csv = local_folder_name + out_file_name

    if not os.path.exists(local_folder_name):
        print("Creating a new local folder for export file:", local_folder_name)
        os.makedirs(local_folder_name)

    # Run SQL script using VSQL
    cmd = ' '.join(["vsql", "-h", VERTICA_HOST, "-d", VERTICA_DB, "-U", VERTICA_USER, "-w", VERTICA_PWD, "-A -F , -t -f",
           sql_script, "-o", output_csv])
    print(cmd)
    subprocess.run(cmd, stderr=subprocess.PIPE)
    print("Extracted data from Vertica written to:", output_csv)

    # Export to S3
    if os.path.isfile(output_csv):
        s3 = resource('s3')
        s3_output = s3_folder_name + out_file_name
        data = open(output_csv, 'rb')
        s3.Bucket(EXPORT_BUCKET).put_object(Key=s3_output, Body=data)
        print("File exported to =>", s3_output)
except subprocess.CalledProcessError as err:
    print("CalledProcessError. Detail =>", err)
    raise
except Exception as err:
    print("Unknown ERROR. Detail =>", err)
    raise



