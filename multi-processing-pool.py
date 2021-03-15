import multiprocessing as mp
import numpy as np
import time
import mysql.connector
from mysql.connector import Error
import requests

no_of_process = 5
items_per_process = 2000

def httpReq(row):
    if not row['smsto']:
        row['smsto'] = '01630132436'

    ploads = {'csms_id': row['id'], 'msisdn': row['smsto'], 'sms': row['message'], 'group': row['group_name']}
    r = requests.get('http://192.168.61.149/ismsdemo/push_test/index.php', params=ploads)
    # r = requests.get('http://192.168.61.149/ismsdemo/push_test/index.php?csms_id=6723&msisdn=None&sms=text160272&group=tasnuva_iftekhairul')

    print(r.text)


def get_result(result):
    global results
    results.append(result)


def pull_isms_data(processId, items):
    try:
        db1 = mysql.connector.connect(host="192.168.61.149", port="3306", user="iftekhairul", password="AbCG@kjh6788P9)", database="sslcare_db")
        db_cursor = db1.cursor(buffered=True, dictionary=True)
        db_cursor.execute("SELECT * FROM isms_data WHERE sms_status = 'processing' AND group_name = 'tasnuva_iftekhairul' AND id >= " + str(processId * items - (items - 1)) + " LIMIT " + str(items_per_process))
        # db_cursor.execute("""SELECT * FROM isms_data WHERE sms_status = %s AND group_name = %s AND id >= %s""", ('processing', 'tasnuva_iftekhairul', (processId * items - (items - 1))))
        result = db_cursor.fetchall()
        return result

    except Error as e:
        print(e)


if __name__ == '__main__':
    results = []

    ts = time.time()
    pool = mp.Pool(mp.cpu_count())

    for i in range(no_of_process):
        processIndex = i + 1
        pool.apply_async(pull_isms_data, args=(processIndex, items_per_process), callback=get_result)

    pool.close()
    pool.join()

    for row in results:
        if row is not None:
            for item in row:
                httpReq(item)

    print('Time in parallel:', time.time() - ts)
    print('\nDone!')