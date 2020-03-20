import datetime
import logging
import re

import psycopg2
from bs4 import BeautifulSoup
import requests
import pika

from config import DB_config


LINKS_IN_SESSION = []
ALL_LINKS = []
conn_DB = None


def save_to_db(url: str, text: str):
    """ Функция для сохранения страницы в БД

    :param url: url о страницы,
    :param text: текст страницы.
    """
    # заменяем символы одинарных кавычек на двойные
    text = text.strip().replace('\'', '"')
    # удаляем часть скрипка, который в теле текства
    text = re.sub(r"\s*window\.dataLayer([^;]*;){4}", "\n", text)
    text = re.sub(r"\s*try{([^}]*}){3}", "\n", text)
    text = re.sub(r"\s*\(function([^;]*;){10}", "\n", text)
    # заменяем повторяющиеся переносы и пробелы на один символ
    text = re.sub(r"\n+", r"\n", text)
    text = re.sub(r" +", r" ", text).strip()
    # формируем запрос
    ins = "insert into url_to_topic values('{0}', '{1}', TIMESTAMP '{2}')".format(url, text,
                                                                                  str(datetime.datetime.today()))
    try:
        with conn_DB.cursor() as cursor:
            cursor.execute(ins)
        conn_DB.commit()
        # Добавляем ссылку в скаченные, если не было ошибки
        ALL_LINKS.append(url)
    except Exception as err:
        logging.error(err)
        conn_DB.rollback()


def main():
    global ALL_LINKS, conn_DB
    conn_rabbit = None
    try:
        conn_DB = psycopg2.connect(**DB_config)
        with conn_DB.cursor() as cursor:
            cursor.execute("select url from url_to_topic")
            links = cursor.fetchall()
        ALL_LINKS = list(link[0] for link in links)
        # Создание очереди
        conn_rabbit = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        channel = conn_rabbit.channel()
        channel.queue_declare(queue='urls',
                              durable=True)  # Сохранение очереди при перезапуске сервера
        # Получение сообщения
        channel.basic_consume(on_message_callback=callback, queue='urls')
        # Запуск бесконечного опроса очереди
        channel.start_consuming()
    except Exception as err:
        logging.error(err)
    finally:
        if conn_DB is not None:
            conn_DB.close()
        if conn_rabbit is not None:
            conn_rabbit.close()


def callback(ch, method, properties, body: bytes):
    url = body.decode()
    print(url)
    if url not in LINKS_IN_SESSION and url not in ALL_LINKS:
        page = requests.get(body)
        soup = BeautifulSoup(page.text, 'lxml')
        save_to_db(url, soup.find('body').get_text())
        LINKS_IN_SESSION.append(url)
    ch.basic_ack(delivery_tag=method.delivery_tag)


if __name__ == "__main__":
    logging.basicConfig(filename="task_2_with_queue.log", level=logging.DEBUG, filemode="w")
    main()
