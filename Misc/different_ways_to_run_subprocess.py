"""
A way to check list of dirs and files in S3 bucket
"""
from account_info import *
import subprocess
import time
import pdb

try:
    sql_script = "sqltest.sql"
    date = time.strftime('%Y%m%d') # YYYMMDD
    output_csv = "test_" + date + ".csv"
    cmd = ["vsql", "-h", VERTICA_HOST, "-d", VERTICA_DB, "-U", VERTICA_USER, "-w", VERTICA_PWD, "-A -F , -t -f",
           sql_script, "-o", output_csv]
    #cmd = "vsql -h 10.252.193.6 -d diap01 -U phyo_thiha -w a_bLDVTw3vkgSK -A -F , -t -f sqltest.sql"
    # cmd = ["vsql", "-h", VERTICA_HOST, "-d", VERTICA_DB, "-U", VERTICA_USER, "-w", VERTICA_PWD, "-A -F , -t -f",
    #        sql_script]
    output = subprocess.check_output(' '.join(cmd),
                                     stderr=subprocess.PIPE)
    ls_of_objs = [s.strip() for s in output.splitlines()]
    pdb.set_trace()
    print("done")
except subprocess.CalledProcessError as err:
    print(err)
    print("ERROR Detail:", err.stderr)
except Exception as err:
    print(err)
    print("ERROR Detail:", err.stderr)