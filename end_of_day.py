from multiprocessing.pool import ThreadPool as Pool
import multiprocessing
from random import randint
from time import sleep
import os
import threading
import csv

def end_of_day():
    with open('After Eod.csv', mode='w') as csv_file:
        fieldnames = ['id', 'Nama', 'Age',	'Balanced',	'No 2b Thread-No', 'No 3 Thread-No', 'Previous Balanced', 'Average Balanced', 'No 1 Thread-No', 'Free Transfer', 'No 2a Thread-No']
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames, delimiter=";")
        writer.writeheader()
        
        f = get_next_row()
        t = Pool(processes=8)
        for i in f:
            t.map(process_average_balance, (i,))
        for i in rows:
            t.map(process_free_transfer_a, (i,))
        for i in rows:
            t.map(process_free_transfer_b, (i,))

        for i in rows:
            t.map(process_bonus_balance, ([i,writer, bonus_remaining],))

        t.close()
        t.join()


def process_average_balance(row):
    row['Average Balanced'] = str((float(row['Balanced']) + float(row['Previous Balanced']))/2)
    row['No 1 Thread-No'] = str(threading.current_thread().ident)
    rows.append(row)

def process_free_transfer_a(row):
    if float(row['Average Balanced']) >=100 and float(row['Average Balanced']) <= 150:
        row['Free Transfer'] = '5'
    row['No 2a Thread-No'] = str(threading.current_thread().ident)

def process_free_transfer_b(row):
    if float(row['Average Balanced']) > 150:
        row['Free Transfer'] = '25'
    row['No 2b Thread-No'] = str(threading.current_thread().ident)

def process_bonus_balance(args):
    row = args[0]
    writer = args[1]
    bonus_remaining = args[2]
    if bonus_remaining.value > 0:
        bonus_remaining.value -= 1
        row['Balanced'] = str(float(row['Balanced']) + 10)
        row['No 3 Thread-No'] = str(threading.current_thread().ident)
    writer.writerow(row)


def get_next_row():
    with open("Before Eod.csv", 'r') as f:
        spamreader = csv.DictReader(f, delimiter=';')
        for row in spamreader:
            yield row

rows = []
bonus_remaining = multiprocessing.Value('i', 100)
end_of_day()
