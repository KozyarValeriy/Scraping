from threading import Thread, Lock
import logging
import time
import re

import psycopg2

from config import URL, DB_config


WORDS_IN_SESSION = []
ALL_WORDS_AND_LINKS = []
LOCK = Lock()
NOT_PAGE = (".png", ".pdf", ".jpeg", ".bmp")


def save_to_db(conn, url: str, word: str, index: int):
    # Запускаем в отдельном потоке, поэтому ставим мьютекс
    if word in ("", " ",):
        return
    if (word, url) in ALL_WORDS_AND_LINKS or (word, url) in WORDS_IN_SESSION:
        return
    ins = "insert into word_to_url values('{0}', '{1}', {2})".format(word, url, index)
    try:
        with conn.cursor() as cursor:
            cursor.execute(ins)
        conn.commit()
        # print(word, url)
    except Exception as err:
        logging.error(err)
        conn.rollback()
        print(err)


def main():
    global ALL_WORDS_AND_LINKS
    conn = None
    try:
        conn = psycopg2.connect(**DB_config)
        with conn.cursor() as cursor:
            cursor.execute("select url, topic from url_to_topic")
            data_to_parse = cursor.fetchall()
        with conn.cursor() as cursor:
            cursor.execute("select word, url from word_to_url")
            ALL_WORDS_AND_LINKS = cursor.fetchall()
        for url, text in data_to_parse:
            for index, word in enumerate(re.split(r"[\n \r\t]", text.lower())):
                save_to_db(conn, url, word, index)
                # thread = Thread(target=save_to_db, args=(conn, url, word, index))
                # thread.setDaemon(True)
                # thread.start()
                WORDS_IN_SESSION.append((word, url))
                # print(word, url)
    except Exception as err:
        logging.error(err)
        # if conn is not None:
        #     conn.rollback()
    finally:
        if conn is not None:
            # conn.commit()
            conn.close()


if __name__ == "__main__":
    logging.basicConfig(filename="task_2.log", level=logging.DEBUG, filemode="w")
    start = time.perf_counter()
    main()
    print("Time spent: {0:6.4f} sec".format(time.perf_counter() - start))