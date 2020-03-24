"""
    Продьюсер для RabbitMG.
    Обходит все страницы сайта, собирает и передает ссылки в очередь.
"""

import logging
import time
import json
import re

import psycopg2
from bs4 import BeautifulSoup
import requests
import pika

from config import URL, DB_config


LINKS_IN_SESSION = set()
ALL_LINKS = set()
NOT_PAGE = (".png", ".pdf", ".jpeg", ".bmp")
# Невидимые элементы
INVISIBLE_ELEMENTS = ('style', 'script', 'head', 'title')


def get_all_links(channel, url: str):
    """ Функция для рекурсивного обхода ссылок на сайте

    :param channel: очередь в  RabbitMQ,
    :param url: url для поиска других ссылок и получения текста страницы.
    """
    # Если в текущей сессии уже был такой url, то возвращаемся, чтобы не обходить его второй раз
    if url in LINKS_IN_SESSION:
        return
    page = requests.get(url)
    soup = BeautifulSoup(page.text, 'lxml')
    # Если страницу по этому url еще нет в БД, то тправляем в очередь
    if url not in ALL_LINKS:

        text = " ".join([s for s in soup.strings if s.parent.name not in INVISIBLE_ELEMENTS])
        # замена одинарных кавычек
        text = text.strip().replace('\'', '"')
        # заменяем повторяющиеся переносы и пробелы на один символ
        text = re.sub(r"\n+", r"\n", text)
        text = re.sub(r" +", r" ", text).strip()
        data = dict(url=url, body=text)
        channel.basic_publish(exchange='',
                              routing_key='urls',
                              properties=pika.BasicProperties(
                                  delivery_mode=2,  # Сохранение сообщения при перезапуске сервера
                              ),
                              body=json.dumps(data))
        print(url)
    # Добавляем эту ссылку в пройденные в этой сессии
    LINKS_IN_SESSION.add(url)
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
            # вызываем функцию для следующего url
            get_all_links(channel, text_link)
        except KeyError:
            pass
        except Exception as err:
            logging.error(err)


def main():
    """ Функция начала обхода.
        Устанавливет соединение с БД и запускает рекурсивную функцию обхода сайта.
    """
    global ALL_LINKS
    conn_DB = None
    conn_rabbit = None
    try:
        conn_DB = psycopg2.connect(**DB_config)
        with conn_DB.cursor() as cursor:
            cursor.execute("select url from url_to_topic")
            links = cursor.fetchall()
        # сохраняем во множество, так как поиск по множеству - O(1)
        ALL_LINKS = set(link[0] for link in links)
        # Создание очереди
        conn_rabbit = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        channel = conn_rabbit.channel()
        channel.queue_declare(queue='urls',
                              durable=True)  # Сохранение очереди при перезапуске сервера
        get_all_links(channel, URL)
    except Exception as err:
        logging.error(err)
    finally:
        if conn_DB is not None:
            conn_DB.close()
        if conn_rabbit is not None:
            conn_rabbit.close()


if __name__ == "__main__":
    logging.basicConfig(filename="task_1_with_queue.log", level=logging.DEBUG, filemode="w")
    start = time.perf_counter()
    main()
    print("Time spent: {0:6.4f} sec".format(time.perf_counter() - start))
