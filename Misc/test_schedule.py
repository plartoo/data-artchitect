import schedule
import time

def main():
    print("This is bob at: ", time.ctime())

if __name__ == "__main__":
    schedule.every(1).minutes.do(main)
    while True:
        schedule.run_pending()
        time.sleep(2)
