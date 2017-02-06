from trigger_on_row_count_change import *

def main():
    table_and_actions = {
        'incampaign_keepingtrac_all':
            [
                {'cmd': ['python', ROOT_FOLDER + 'rentrak_step1.py']},
            ],
        'incampaign_rentrak_zipcode': # we will watch zipcode table because its loads more data and is slower to complete
            [
                {'cmd': ['python', ROOT_FOLDER + 'rentrak_step2.py']},
            ],
    }
    trigger_on_row_count_change(table_and_actions, 2)

if __name__ == "__main__":
    main()
