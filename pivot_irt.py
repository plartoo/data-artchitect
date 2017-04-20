#!/usr/bin/env python
"""
Description: Script to launch pivotting code for IRT table using
a combination of Campaign and VariableName columns.
"""

import time
import re

from account_info import *
from vertica_utils import *


__author__ = "Phyo Thiha"
__last_modified__ = "April 20, 2017"
__status__ = "Staging"


def prepare_case_statements():
    return """
        SELECT DISTINCT
          REGEXP_REPLACE(
            REGEXP_REPLACE(
              '(case when Campaign=''mycampaign'' and VariableName=''myvariable_name'' then VariableValue else 0 end) as mycampaign_myvariable_name,'
              , 'mycampaign', Campaign )
          , 'myvariable_name', VariableName )
        FROM gaintheory_us_targetusa_14.incampaign_IRT_all_last_60_days;
    """


def main():
    with vertica_python.connect(**conn_info) as connection:

        cur = connection.cursor()
        rows = get_rows(cur, prepare_case_statements())
        sql1 = """
            SELECT Geography, Period, Product, Outlet, Creative,
        """

        sql2 = ''.join([i for sublist in rows for i in sublist])[:-1] # removed the last comma
        pat = r'(end\) as )([^,]*)' #r'(?<=end\) as )[\w\s]*(?=,)'
        sql2 = re.sub(pat, lambda m: "{}{}".format(m.group(1), re.sub(r'\W+', '_', m.group(2))), sql2)

        sql3 = """
            INTO gaintheory_us_targetusa_14.incampaign_IRT_all_last_60_days_pivoted
            FROM gaintheory_us_targetusa_14.incampaign_IRT_all_last_60_days
        """

        print(sql1+sql2+sql3)
        t1 = time.clock()
        # text_file = open('pivotsql.sql', 'w')
        # text_file.write(sql1+sql2+sql3)
        # text_file.close()
        cur.execute(sql1+sql2+sql3)
        connection.commit()
        t2 = time.clock()
        print(round(t2 - t1, 3))


if __name__ == "__main__":
    main()
