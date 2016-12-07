from trigger_on_row_count_change import *


def main():
    table_and_actions = {
        'incampaign_storesales_zipcode':
            [
                {'cmd': ['python', ROOT_FOLDER + 'run_vsql.py', SQL_SCRIPT_FOLDER + 'process_storesales.sql']},
                # {'cmd': ['python', ROOT_FOLDER + 'archive_files.py', 'FilesForDatamart/StoreSales/',
                #          S3_ARCHIVE_ROOT + 'StoreSales/']},
                {'cmd': ['python', ROOT_FOLDER + 'run_vsql_and_export_to_s3.py',
                         SQL_SCRIPT_FOLDER + 'export_storesales.sql',
                         'StoreSales/', 'FilesForDatamart/StoreSales/', 'storesales']}
            ]
    }
    trigger_on_row_count_change(table_and_actions)

if __name__ == "__main__":
    main()
