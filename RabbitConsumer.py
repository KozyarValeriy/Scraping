"""
    Консьюмер для RabbitMG.
    Получает ссылку на сайт. Сохраняет всю статью и сразу строит обратный
    индекс (слово -> сылка на статью).
"""

import datetime
import logging
import re

import psycopg2
from bs4 import BeautifulSoup
import requests
import pika

from config import DB_config


ALL_LINKS = set()

WORDS_IN_SESSION = set()
ALL_WORDS_AND_LINKS = set()

conn_DB = None


def save_to_db(url: str, text: str) -> str:
    """ Функция для сохранения страницы в БД

    :param url: url о страницы,
    :param text: текст страницы.
    :return: обработанный текст статьи.
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
        # Добавляем ссылку в скаченные
        ALL_LINKS.add(url)
    except Exception as err:
        logging.error(err)
    return text


def save_word_in_db(url: str, word: str):
    """ Функция для сохранения слов в БД """
    ins = "insert into word_to_url values('{0}', '{1}')".format(word, url)
    try:
        with conn_DB.cursor() as cursor:
            cursor.execute(ins)
    except Exception as err:
        logging.error(err)
        print(err)


def main():
    """ Главная функция.
        Инициализирует подключение к БД и очереди.
        Опрашивает очередь и записывает полученные статьи в БД.
    """
    global ALL_LINKS, conn_DB, ALL_WORDS_AND_LINKS
    conn_rabbit = None
    try:
        conn_DB = psycopg2.connect(**DB_config)
        # Получаем все статьи, которые есть на текущий момент в БД
        with conn_DB.cursor() as cursor:
            cursor.execute("select url from url_to_topic")
            links = cursor.fetchall()
            # сохраняем во множество, так как поиск по множеству - O(1)
            ALL_LINKS = set(link[0] for link in links)
        # Получаем все уже записанные слова в БД, чтобы не писать их еще раз
        with conn_DB.cursor() as cursor:
            cursor.execute("select word, url from word_to_url")
            ALL_WORDS_AND_LINKS = set(cursor.fetchall())
        # Создание очереди
        conn_rabbit = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        channel = conn_rabbit.channel()
        channel.queue_declare(queue='urls',
                              durable=True)  # Сохранение очереди при перезапуске сервера
        # Получение сообщения
        channel.basic_consume(on_message_callback=handing_message, queue='urls')
        # Запуск бесконечного опроса очереди
        channel.start_consuming()
    except Exception as err:
        logging.error(err)
    finally:
        if conn_DB is not None:
            conn_DB.close()
        if conn_rabbit is not None:
            conn_rabbit.close()


def handing_message(ch, method, properties, body: bytes):
    """ Функция для обработки одонго сообщения из очереди """
    # преобразование в строку
    url = body.decode()
    print(url)
    # Записываем только те ссылки, которых еще нет в БД
    if url not in ALL_LINKS:
        page = requests.get(body)
        soup = BeautifulSoup(page.text, 'lxml')
        page = save_to_db(url, soup.find('body').get_text().lower())
        # для всех слов в тексте
        for word in set(re.split(r"\W", page)):
            if word in ("", " "):
                continue
            if (word, url) in ALL_WORDS_AND_LINKS:
                continue
            save_word_in_db(url, word)
            ALL_WORDS_AND_LINKS.add((word, url))
        conn_DB.commit()
    ch.basic_ack(delivery_tag=method.delivery_tag)


if __name__ == "__main__":
    logging.basicConfig(filename="task_2_with_queue.log", level=logging.DEBUG, filemode="w")
    main()
