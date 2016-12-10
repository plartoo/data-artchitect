
# coding: utf-8

import pandas as pd
import vertica_python
import os


# Vertica access details
vertica_user = 'manoj_kr'
vertica_pass = 'Gaintheory@123'

# Vertica connection string
conn_info = {
    'host': '10.252.192.8',
    'port': 5433,
    'user': vertica_user,
    'password': vertica_pass,
    'database': 'diap01',
    'read_timeout': 4000,
    'unicode_error': 'strict',
    'ssl': False
}

# Analysis start and end dates # (last six week)
start_date = '2016-10-27'
end_date = '2016-12-08'

# Location of sources and destination files
data_folder = 'C:/Users/phyo.thiha/Desktop/VaultToDatamart/'#r'H:\Target\2016\zip code level\Jonathan'

# Names of files, matching file generated based on start and end dates
kt_cleaned = r'kt_cleaned_%s_%s.xlsx' % (start_date, end_date)
matching_file = r'creative_matching_%s_%s.xlsx' % (start_date, end_date)

# Function to extract data from vertica into a pandas dataframe
def vertica_extract(query, columns, index=None):
    with vertica_python.connect(**conn_info) as connection:
        cur = connection.cursor()
        cur.execute(query)
        results = pd.DataFrame(cur.fetchall())
        results.columns = columns
        if index:
            return results.set_index(index)
        else:
            return results


# Step 1: Download all possible KT combinations and current matching cleaned creative names

extract_query = """
SELECT Air_ISCI as kt_creative_id, Cmml_Title AS kt_creative, kt_creative_clean
FROM gaintheory_us_targetusa_14.Keepingtrac_backup a
LEFT JOIN gaintheory_us_targetusa_14.kt_creative_cleaned b
ON a.Air_ISCI = b.kt_creative_id
WHERE Air_ISCI IS NOT NULL
GROUP BY a.Air_ISCI, a.Cmml_Title, kt_creative_clean
ORDER BY kt_creative_id
"""

df = vertica_extract(
    extract_query,
    ['kt_creative_id', 'kt_creative', 'kt_creative_clean']
).to_excel(
    os.path.join(data_folder, kt_cleaned),
    sheet_name='creative_cleaning',
    index=False
)


# Step 2: Upload the cleaned creative names after blanks have been filled in

creation_query = """
    DROP TABLE IF EXISTS gaintheory_us_targetusa_14.kt_creative_cleaned;
    CREATE TABLE gaintheory_us_targetusa_14.kt_creative_cleaned (
            kt_creative_id   varchar(150),
            kt_creative_clean varchar(1500)
    );
"""

insert_query = """
    INSERT INTO gaintheory_us_targetusa_14.kt_creative_cleaned(kt_creative_id, kt_creative_clean) VALUES ('%s', '%s')
"""

with vertica_python.connect(**conn_info) as connection:
    cur = connection.cursor()
    cur.execute(creation_query)
    for row in pd.read_excel(os.path.join(data_folder, kt_cleaned), sheetname='creative_cleaning').iterrows():
        cur.execute(insert_query % (row[1]['kt_creative_id'], row[1]['kt_creative_clean']))
    connection.commit()


# # Step 3: Download the raw creative details from both KT and RT and match based on network and minute
#
# creative_query = """
# SELECT rt_creative_id, rt_creative, kt_creative_id, kt_creative, SUM(rt_imp) AS rt_imp, SUM(kt_imp) AS kt_imp
# FROM (
#         SELECT DATE_TRUNC('minute', Air_Date + Air_Time) AS air_date, rt_network AS network, kt_creative_id, kt_creative, SUM(Act_Impression) as kt_imp
#         FROM gaintheory_us_targetusa_14.kt_with_rt_network_mk
#         WHERE Air_Date BETWEEN '""" + start_date + """' AND '""" + end_date + """'
#         GROUP BY DATE_TRUNC('minute', Air_Date + Air_Time), network, kt_creative_id, kt_creative
# ) kt
# FULL OUTER JOIN(
#         SELECT DATE_TRUNC('minute', rentrak_ad_time) AS air_date, rentrak_network as network, rentrak_ad_no AS rt_creative_id, rentrak_ad_copy AS rt_creative, SUM(a.rentrak_ad_zip_aa) AS rt_imp
#         FROM gaintheory_us_targetusa_14.incampaign_rentrak_zipcode a
#         LEFT JOIN gaintheory_us_targetusa_14.incampaign_rentrak_spotid b
#         ON a.rentrak_spot_id = b.rentrak_spot_id and a.rentrak_week = b.rentrak_week
#         WHERE rentrak_ad_time::date BETWEEN '""" + start_date + """' AND '""" + end_date + """'
#         GROUP BY DATE_TRUNC('minute', rentrak_ad_time), network, rt_creative_id, rt_creative
# ) rt
# ON kt.air_date = rt.air_date AND kt.network = rt.network
# WHERE  rt_creative_id IS NOT NULL
# GROUP BY rt_creative_id, rt_creative, kt_creative_id, kt_creative
# ORDER BY rt_creative_id, kt_creative_id;
# """
#
# data = vertica_extract(creative_query, ['rt_creative_id', 'rt_creative', 'kt_creative_id', 'kt_creative', 'rt_imp', 'kt_imp'])
# dupes = data.copy()
# dupes['count'] = 1
# dupes = dupes.loc[:, ['rt_creative_id', 'count']].rename(columns={'count': 'dupe'})
# dupes = dupes.groupby('rt_creative_id').sum()
# data = pd.merge(data, dupes, left_on='rt_creative_id', right_index=True)
# data.to_excel(
#     os.path.join(data_folder, matching_file),
#     sheet_name='Dedupe',
#     index=False
# )
#
# # Step 4: Upload the deduped sheet of results to new table
#
# table_name = 'gaintheory_us_targetusa_14.js_creative_match_deduped_%s_%s' % (start_date.replace('-',''), end_date.replace('-',''))
#
# creation_query = """
#     DROP TABLE IF EXISTS """ + table_name + """;
#     CREATE TABLE """ + table_name + """(
#             rt_creative_id   integer,
#             rt_creative      varchar(1500),
#             kt_creative_id   varchar(150),
#             kt_creative varchar(1500)
#     );
# """
#
# insert_query = """
#     INSERT INTO """ + table_name + """(rt_creative_id, rt_creative, kt_creative_id, kt_creative) VALUES ('%s', '%s', '%s', '%s')
# """
#
# with vertica_python.connect(**conn_info) as connection:
#     cur = connection.cursor()
#     cur.execute(creation_query)
#     for row in pd.read_excel(os.path.join(data_folder, matching_file), sheetname='Dedupe').iterrows():
#         cur.execute(insert_query % (row[1]['rt_creative_id'], row[1]['rt_creative'], row[1]['kt_creative_id'], row[1]['kt_creative']))
#     connection.commit()
#
