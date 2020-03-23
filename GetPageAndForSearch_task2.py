"""
    Скрипт второй части паука.
    Обходим все скаченные статьи и строим обратный индекс: слово -> сылка на статью.
    Так как страницы уже скачены, то запросы снова делать не нужно.
"""

import logging
import time
import re

import psycopg2

from config import DB_config


NOT_PAGE = (".png", ".pdf", ".jpeg", ".bmp")
ALL_WORDS_AND_LINKS = None


def save_to_db(conn, url: str, word: str):
    """ Функция для сохранения в БД """
    ins = "insert into word_to_url values('{0}', '{1}')".format(word, url)
    try:
        with conn.cursor() as cursor:
            cursor.execute(ins)
    except Exception as err:
        logging.error(err)
        print(err)


def main():
    """ Функция обхода статей по словам. """
    global ALL_WORDS_AND_LINKS
    conn = None
    try:
        i = 0
        conn = psycopg2.connect(**DB_config)
        # Получаем все статьи, которые есть на текущий момент в БД
        with conn.cursor() as cursor:
            cursor.execute("select url, topic from url_to_topic")
            data_to_parse = cursor.fetchall()
        # Получаем все уже записанные слова в БД, чтобы не писать их еще раз
        with conn.cursor() as cursor:
            cursor.execute("select word, url from word_to_url")
            # сохраняем во множество, так как поиск по множеству - O(1)
            ALL_WORDS_AND_LINKS = set(cursor.fetchall())
        max_ = len(data_to_parse)  # кол-во всех ссылок. Для дебага
        for url, text in data_to_parse:
            i += 1  # Для дебага
            # Разбиваем статью на слова
            for word in set(re.split(r"\W", text.lower())):
                if word in ("", " "):
                    continue
                if (word, url) in ALL_WORDS_AND_LINKS:
                    continue
                save_to_db(conn, url, word)
                ALL_WORDS_AND_LINKS.add((word, url))
            print("{0} from {1} done! url: {2}".format(i, max_, url))  # Для дебага
    except Exception as err:
        logging.error(err)
    finally:
        if conn is not None:
            conn.commit()
            conn.close()


if __name__ == "__main__":
    logging.basicConfig(filename="task_2.log", level=logging.DEBUG, filemode="w")
    start = time.perf_counter()
    main()
    print("Time spent: {0:6.4f} sec".format(time.perf_counter() - start))
