from collections import deque
import datetime
from threading import Thread, Lock
import logging

import psycopg2
from bs4 import BeautifulSoup
import requests

from config import URL, DB_config

# all_links = dict()
LINKS_IN_SESSION = []
ALL_LINKS = []
links_queue = deque()
LOCK = Lock()
logging.basicConfig(filename="task_1.log", level=logging.DEBUG, filemode="w")


def save_to_db(conn, url: str, text: str):
    """ Функция для сохранения страницы в БД """
    LOCK.acquire()
    ins = "insert into url_to_topic values('{0}', '{1}', TIMESTAMP '{2}')".format(url, text,
                                                                                  str(datetime.datetime.today()))
    try:
        with conn.cursor() as cursor:
            cursor.execute(ins)
        conn.commit()
        logging.DEBUG("Запись в БД - ОК!")
    except Exception as err:
        print(f"ERROR:")
        print(err)
        print(url)
        print(text)
        logging.error(err)
        conn.rollback()
        exit(1)
    finally:
        LOCK.release()


def get_all_link(conn, url: str):
    if url in LINKS_IN_SESSION:
        return
    page = requests.get(url)
    soup = BeautifulSoup(page.text, 'lxml')
    if url not in ALL_LINKS:
        thread = Thread(target=save_to_db, args=(conn, url, soup.get_text().replace('\'', '"')))
        thread.setDaemon(True)
        thread.start()
        ALL_LINKS.append(url)
    LINKS_IN_SESSION.append(url)
    for link in set(soup.findAll("a")):
        try:
            text_link = str(link["href"])
            if not text_link.startswith(('/', URL)):
                continue
            text_link = text_link if text_link.startswith(URL) else "".join([URL, text_link])
            text_link = text_link.rstrip('/')
            if text_link not in LINKS_IN_SESSION:
                get_all_link(conn, text_link)
                # links_queue.append(text_link)
        except KeyError:
            pass
        except Exception as err:
            logging.error(err)


def main():
    global ALL_LINKS
    conn = None
    try:
        conn = psycopg2.connect(**DB_config)
        with conn.cursor() as cursor:
            cursor.execute("select url from url_to_topic")
            links = cursor.fetchall()
        ALL_LINKS = list(link[0] for link in links)
        get_all_link(conn, URL)
    finally:
        if conn is not None:
            conn.close()


main()
