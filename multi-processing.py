import multiprocessing
import mysql.connector
from mysql.connector import Error
import requests
import time

noOfProcess = 5
itemsPerProcess = 50

def httpReq(row):
    if not row['smsto']:
        row['smsto'] = '01630132436'

    ploads = {'csms_id': row['id'], 'msisdn': row['smsto'], 'sms': row['message'], 'group': row['group_name']}
    r = requests.get('http://192.168.61.149/ismsdemo/push_test/index.php', params=ploads)
    # r = requests.get('http://192.168.61.149/ismsdemo/push_test/index.php?csms_id=6723&msisdn=None&sms=text160272&group=tasnuva_iftekhairul')

    if r.text == 'SUCCESS':
        update_isms_data(row)


def update_isms_data(row):
    # print(row['id'])
    db1 = mysql.connector.connect(host="192.168.61.149", port="3306", user="iftekhairul", password="AbCG@kjh6788P9)",
                                  database="sslcare_db")
    db_cursor = db1.cursor()
    db_cursor.execute("UPDATE isms_data SET sms_status = 'processing' WHERE id = " + str(row['id']))
    db1.commit()
    print(db_cursor.rowcount, "record(s) affected")

def pull_isms_data(processId, items):
    try:
        db1 = mysql.connector.connect(host="192.168.61.149", port="3306", user="iftekhairul", password="AbCG@kjh6788P9)", database="sslcare_db")
        db_cursor = db1.cursor(buffered=True, dictionary=True)
        db_cursor.execute("SELECT * FROM isms_data WHERE sms_status = 'processing' AND group_name = 'tasnuva_iftekhairul' AND id >= " + str(processId * items - (items - 1)) + " LIMIT " + str(itemsPerProcess))
        result = db_cursor.fetchall()
        print("\n")

        # httpReq(result)

        for row in result:
            # print(row)
            httpReq(row)

        print("\n")

    except Error as e:
        print(e)


if __name__ == "__main__":
    start = time.perf_counter()
    for i in range(noOfProcess):
        processIndex = i + 1
        process = multiprocessing.Process(target=pull_isms_data, args=(processIndex, itemsPerProcess))
        process.start()
        process.join()

    # processes finished
    print("Done!")
    finish = time.perf_counter()
    print(f'finished in {round(finish - start, 2)} seconds(s)')