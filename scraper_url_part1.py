"""
    Скрипт первой части паука.
    Обходит все старницы сайта и сохряняет их в базу данных.
"""

from threading import Thread, Lock
import logging
import time
import re

import psycopg2
from bs4 import BeautifulSoup
import requests

from config import URL, DB_config


LINKS_IN_SESSION = set()
ALL_LINKS = set()
LOCK = Lock()
NOT_PAGE = (".png", ".pdf", ".jpeg", ".bmp")
ALL_THREAD = []
# Невидимые элементы
INVISIBLE_ELEMENTS = ('style', 'script', 'head', 'title')


def save_to_db(conn, url: str, text: str):
    """ Функция для сохранения страницы в БД

    :param conn: соединение с БД,
    :param url: url о страницы,
    :param text: текст страницы.
    """
    # Запускаем в отдельном потоке, поэтому ставим мьютекс
    LOCK.acquire()
    # заменяем символы одинарных кавычек на двойные
    text = text.strip().replace('\'', '"')
    # заменяем повторяющиеся переносы и пробелы на один символ
    text = re.sub(r"\n+", r"\n", text)
    text = re.sub(r" +", r" ", text).strip()
    # формируем запрос
    ins = "insert into url_to_topic values('{0}', '{1}')".format(url, text)
    try:
        with conn.cursor() as cursor:
            cursor.execute(ins)
        # Добавляем ссылку в скаченные, если не было ошибки
        ALL_LINKS.add(url)
        # print(url)
    except Exception as err:
        logging.error(err)
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
        text = ' '.join([s for s in soup.strings if s.parent.name not in INVISIBLE_ELEMENTS])
        ALL_THREAD.append(Thread(target=save_to_db, args=(conn, url, text)))
        ALL_THREAD[-1].start()
    # Добавляем эту ссылку в пройденные в этой сессии
    LINKS_IN_SESSION.add(url)
    # Обходим все теги <a> на странице
    for link in set(soup.findAll("a")):
        try:
            text_link = str(link["href"])
            # Если это не ссылка на этот же сайт или это не страница, то пропускаем
            if not text_link.startswith(('/', URL)) or text_link.endswith(NOT_PAGE):
                print(text_link)
                continue
            # Для относительных ссылок добавляем в начало главный url
            text_link = text_link if text_link.startswith(URL) else "".join([URL, text_link])
            text_link = text_link.rstrip('/')
            # вызываем функцию для следующего url
            get_all_link(conn, text_link)
        except KeyError:
            pass
        except Exception as err:
            logging.error(err)


def main():
    """ Функция начала обхода.
        Устанавливет соединение с БД и запускает рекурсивную функцию обхода сайта.
    """
    global ALL_LINKS
    conn = None
    try:
        conn = psycopg2.connect(**DB_config)
        # Запрос к БД, чтобы узнать, какие страницы уже сохранены сейчас
        with conn.cursor() as cursor:
            cursor.execute("select url from url_to_topic")
            links = cursor.fetchall()
        # сохраняем во множество, так как поиск по множеству - O(1)
        ALL_LINKS = set(link[0] for link in links)
        get_all_link(conn, URL)
    except Exception as err:
        logging.error(err)
    finally:
        if conn is not None:
            for thread in ALL_THREAD:
                thread.join()
            conn.commit()
            conn.close()


if __name__ == "__main__":
    logging.basicConfig(filename="task_1.log", level=logging.DEBUG, filemode="w")
    start = time.perf_counter()
    main()
    print("Time spent: {0:6.4f} sec".format(time.perf_counter() - start))
