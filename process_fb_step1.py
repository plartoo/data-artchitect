from trigger_on_row_count_change import *


def main():
    table_and_actions = {
        'incampaign_facebook_impressions_and_spend':
            [
                {'cmd': ['python', ROOT_FOLDER+'run_vsql.py', SQL_SCRIPT_FOLDER+'fb_extract_gp_names.sql']},
                {'cmd': ['python', ROOT_FOLDER + 'fb_step1_post_process.py']}
            ]
    }
    trigger_on_row_count_change(table_and_actions, 2)

if __name__ == "__main__":
    main()
