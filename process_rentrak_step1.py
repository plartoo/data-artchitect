from trigger_on_row_count_change import *

def main():
    table_and_actions = {
        'keepingtrac':
            [
                {'cmd': ['python', ROOT_FOLDER + 'rentrak_step1.py']},
            ],
    }
    trigger_on_row_count_change(table_and_actions, 2)

if __name__ == "__main__":
    main()
