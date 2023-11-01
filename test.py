from threading import Thread
import time


def show_timer():
    count = 0
    while True:
        count += 1
        time.sleep(1)
        print(f'Прошло {count} секунд...')


t = Thread(target=show_timer, daemon=True)
t.start()

answer = input('Вы хотите выйти?\n')

