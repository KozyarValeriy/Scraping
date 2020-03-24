"""
    Консьюмер для RabbitMG.
    Получает ссылку на сайт. Сохраняет всю статью и сразу строит обратный
    индекс (слово -> сылка на статью).
"""

import logging
import re
import json

import psycopg2
import pika

from config import DB_config


ALL_LINKS = set()

WORDS_IN_SESSION = set()
ALL_WORDS_AND_LINKS = set()
# Невидимые элементы
INVISIBLE_ELEMENTS = ('style', 'script', 'head', 'title')
conn_DB = None


def save_to_db(action: str) -> bool:
    """ Метод для выполнения строки action в БД

    :param action: строка для выполнения в БД,
    :return: результат выполнения.
    """
    try:
        with conn_DB.cursor() as cursor:
            cursor.execute(action)
        return True
    except Exception as err:
        logging.error(err)
        return False


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
    data = json.loads(body)
    print(data["url"])
    # Записываем только те ссылки, которых еще нет в БД
    if data["url"] not in ALL_LINKS:
        # запрос на вставку
        insert = "insert into url_to_topic values('{0}', '{1}')".format(data["url"], data["body"])
        res = save_to_db(insert)
        if res:
            ALL_LINKS.add(data["url"])
        # для всех слов в тексте
        for word in set(re.split(r"\W", data["body"].lower())):
            if word in ("", " "):
                continue
            if (word, data["url"]) in ALL_WORDS_AND_LINKS:
                continue
            insert = "insert into word_to_url values('{0}', '{1}')".format(word, data["url"])
            res = save_to_db(insert)
            if res:
                ALL_WORDS_AND_LINKS.add((word, data["url"]))
        conn_DB.commit()
    ch.basic_ack(delivery_tag=method.delivery_tag)


if __name__ == "__main__":
    logging.basicConfig(filename="task_2_with_queue.log", level=logging.DEBUG, filemode="w")
    main()
