import threading
import mysql.connector
from mysql.connector import Error
import time

noOfThread = 10
itemsPerThread = 50
exitFlag = 0



class IsmsThread (threading.Thread):

   def __init__(self, threadId, name):
      threading.Thread.__init__(self)
      self.threadId = threadId
      self.name = name
      self.items = itemsPerThread

   def run(self):
      pull_isms_data(self.threadId, self.name, self.items)


def pull_isms_data(threadId, threadName, items):
    start = time.perf_counter()
    try:
        db1 = mysql.connector.connect(host="localhost", user="smiftakhairul", password="siceptix", database="ismsdb_main")
        db_cursor = db1.cursor()
        db_cursor.execute("SELECT * FROM isms_data WHERE id >= "+ str(threadId * items - (items - 1)) +" LIMIT "+ str(items))
        result = db_cursor.fetchone()
        print(threadName)
        print("\n")
        print(result)
        print("\n")
        finish = time.perf_counter()
        print(f'finished in {round(finish - start, 2)} seconds(s)')



    except Error as e:
        print(e)


threads = []


for i in range(noOfThread):
    threadIndex = i + 1
    thread = IsmsThread(threadIndex, "Thread-" + str(threadIndex))
    threads.append(thread)
    thread.start()




