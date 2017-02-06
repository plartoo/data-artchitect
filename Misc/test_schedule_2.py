import schedule
import time

def main():
    print("This is lacy at: ", time.ctime())

if __name__ == "__main__":
    schedule.every(2).minutes.do(main)
    while True:
        schedule.run_pending()
        time.sleep(1)
