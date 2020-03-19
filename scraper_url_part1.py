import datetime
from threading import Thread, Lock
import logging
import time
import re

import psycopg2
from bs4 import BeautifulSoup
import requests

from config import URL, DB_config

# all_links = dict()
LINKS_IN_SESSION = []
ALL_LINKS = []
LOCK = Lock()
NOT_PAGE = (".png", ".pdf", ".jpeg", ".bmp")


def save_to_db(conn, url: str, text: str):
    """ Функция для сохранения страницы в БД

    :param conn: соединение с БД,
    :param url: url о страницы,
    :param text: текст страницы.
    """
    # Запускаем в отдельном потоке, поэтому ставим мьютекс
    LOCK.acquire()
    text = text.strip()
    text = re.sub(r"\n+", r"\n", text)
    ins = "insert into url_to_topic values('{0}', '{1}', TIMESTAMP '{2}')".format(url, text,
                                                                                  str(datetime.datetime.today()))
    try:
        with conn.cursor() as cursor:
            cursor.execute(ins)
        conn.commit()
        # Добавляем ссылку в скаченные, если не было ошибки
        ALL_LINKS.append(url)
    except Exception as err:
        logging.error(err)
        # conn.rollback()
    finally:
        LOCK.release()


def get_all_link(conn, url: str):
    """ Функция для рекурсивного обхода ссылок на сайте

    :param conn: соединение с БД,
    :param url: url для поиска других ссылок и получения текста страницы.
    """
    # Если в текущей сессии уже был такой url, то возвращаемся, чтобы не обходить его второй раз
    if url in LINKS_IN_SESSION:
        return
    page = requests.get(url)
    soup = BeautifulSoup(page.text, 'lxml')
    # Если страницу по этому url еще нет в БД, то записываем ее в БД
    if url not in ALL_LINKS:
        thread = Thread(target=save_to_db, args=(conn, url, soup.get_text().replace('\'', '"')))
        thread.setDaemon(True)
        thread.start()
    # Добавляем эту ссылку в пройденные в этой сессии
    LINKS_IN_SESSION.append(url)
    # Обходим все теги <a> на странице
    for link in set(soup.findAll("a")):
        try:
            text_link = str(link["href"])
            # Если это не ссылка на этот же сайт или это не страница, то пропускаем
            if not text_link.startswith(('/', URL)) or text_link.endswith(NOT_PAGE):
                continue
            # Для относительных ссылок добавляем в начало главный url
            text_link = text_link if text_link.startswith(URL) else "".join([URL, text_link])
            text_link = text_link.rstrip('/')
            # if text_link not in LINKS_IN_SESSION:
            # ызываем функцию для следующего url
            get_all_link(conn, text_link)
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
        print(ALL_LINKS)
        print(len(ALL_LINKS))
        get_all_link(conn, URL)
        print(LINKS_IN_SESSION)
        print(len(LINKS_IN_SESSION))
    except Exception as err:
        logging.error(err)
        # if conn is not None:
        #     conn.rollback()
    finally:
        if conn is not None:
            # conn.commit()
            conn.close()


if __name__ == "__main__":
    logging.basicConfig(filename="task_1.log", level=logging.DEBUG, filemode="w")
    start = time.perf_counter()
    main()
    print("Time spent: {0:6.4f} sec".format(time.perf_counter() - start))
