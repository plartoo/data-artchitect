import os

from account_info import *
from boto3 import client, resource


def list_key_names(folder):
    """
    :return: A list of S3 keys such as ['Facebook/websales_20161202.csv', ...]
    """
    file_names = []

    s3_conn = client('s3')
    for key in s3_conn.list_objects(Bucket=MAIN_BUCKET, Prefix=folder)['Contents']:
        name = str(key['Key'])
        if name != folder:
            file_names.append(name)
    return file_names


def list_file_names(folder):
    file_names = []

    s3_conn = client('s3')
    for key in s3_conn.list_objects(Bucket=MAIN_BUCKET, Prefix=folder)['Contents']:
        name = str(key['Key'])
        if name != folder:
            file_names.append(name.replace(folder, ''))
    return file_names


def archive_files(from_folder, to_folder, new_file_postfix=''):
    print("Archiving files within S3 folder:", from_folder)
    for f_name in list_file_names(from_folder):
        source_key = from_folder + f_name
        f_name_without_extension = os.path.splitext(f_name)[0]
        f_extension = os.path.splitext(f_name)[1]
        dest_key = to_folder + f_name_without_extension + new_file_postfix + f_extension

        archive_file(source_key, dest_key)


def archive_file(source_key, dest_key):
    s3 = resource('s3')

    s3.Object(MAIN_BUCKET, dest_key).copy_from(CopySource={'Bucket': MAIN_BUCKET, 'Key': source_key})
    print("Deleting:", source_key)

    s3.Object(MAIN_BUCKET, source_key).delete()
    print("Archived this key:\t", source_key, "\tto\t", dest_key)
