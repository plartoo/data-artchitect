"""
Run VSQL via Python subprocess.
"""
import sys
import subprocess

from account_info import *

try:
    sql_script = sys.argv[1]        # e.g., "sqltest.sql"

    # -t if included will remove headers and the last line that has total row number
    cmd = ' '.join(["vsql", "-h", VERTICA_HOST, "-d", VERTICA_DB, "-U", VERTICA_USER,
                    "-w", VERTICA_PWD, "-f", sql_script])
    subprocess.run(cmd, stderr=subprocess.PIPE)
    print("Finished running VSQL command: ", cmd)
except subprocess.CalledProcessError as err:
    print("CalledProcessError. Detail =>", err)
    raise
except Exception as err:
    print("Unknown ERROR. Detail =>", err)
    raise



