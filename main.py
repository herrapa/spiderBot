import argparse
import urllib
from urllib.parse import urlparse, urljoin, urlsplit, urlunsplit
import urllib.request
from bs4 import BeautifulSoup
import sqlite3 as lite
from multiprocessing import Process, Lock, Queue
import time

parser = argparse.ArgumentParser(description='Scan ze webz')
parser.add_argument('URLs', metavar='U', type=str, nargs='+',
                   help='URLS to start off with')

WORKER_THREADS = 10

def process_url(lock, url_queue, t_num):
    connection = lite.connect('test.db')
    while True:
        print ("Thread: ", t_num)
        if not url_queue.empty():
            url = url_queue.get()
            lock.acquire()
            cursor = connection.cursor()
            cursor.execute('insert into Data values(NULL,?)', (url,))
            connection.commit()
            lock.release()
            for u in parse_urls(url):
                # in mem or in database? depth first or??
                lock.acquire()
                cursor.execute("SELECT count(*) FROM Data WHERE Url = ?", (u,))
                data=cursor.fetchone()[0]
                if data==0:
                    url_queue.put(u)
                lock.release()
        else:
            print ("No more URLs :( ", t_num)
            #time.sleep(1)

def parse_urls(url):
    print("URL: ", url)
    try:
        with urllib.request.urlopen(url) as response:
            html = response.read()
            soup = BeautifulSoup(html)
            links = soup.find_all('a')
            new_urls = set([])
            for tag in links:
                link = tag.get('href',None)
                if link is not None:
                    new_url = ""
                    if link.startswith("http"):
                        new_url = link
                    elif link.startswith("/"):
                        new_url = urljoin(url, link)
                    else:
                        continue
                    new_url = clean_url(new_url)

                    if not is_file(new_url):
                        new_urls.add(new_url)
            return list(new_urls)
    except Exception as e:
        print ("Something went bad!", e, end='\n')

    return []

def clean_url(url):
    if "?" in url:
        url = url[:url.find("?")]
    if "#" in url:
        url = url[:url.find("#")]
    return url

def is_file(url):
    url = url.lower()
    return any([url.endswith(x) for x in ["jpg", "png", "mp4", "mp3", "wav"]])

if __name__ == '__main__':
    print ("Bot starting!")
    args = parser.parse_args()
    url_queue = Queue()
    for u in args.URLs:
        url_queue.put(u)

    connection = lite.connect('test.db')
    try:
        cursor = connection.cursor()
        cursor.execute("CREATE TABLE IF NOT EXISTS Data(Id INT PRIMARY KEY, Url TEXT UNIQUE ON CONFLICT IGNORE)")

        lock = Lock()
        processes = []
        #with Pool(processes = WORKER_THREADS) as pool:
        for num in range(WORKER_THREADS):
            print ("Starting thread: ", num)
            p = Process(target=process_url, args=(lock, url_queue, num))
            p.start()
            processes.append(p)
            #pool.apply_async(process_url, args=(lock, url_queue, num, connection))
        while True:
            i = input("q to quit")
            if i == 'q':
                break
        #pool.close()
        #pool.join()
        for p in processes:
            print ("Alive", p.is_alive())
            p.terminate() #might cause deadlocks!?

    except lite.Error as e:
        print ("Error %s:" % e.args[0])
        sys.exit(1)

    finally:
        print ("Closing connection")
        if connection:
            connection.close()
