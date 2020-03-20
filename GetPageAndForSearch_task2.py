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


def save_to_db(conn, url: str, word: str):
    ins = "insert into word_to_url values('{0}', '{1}')".format(word, url)
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
        i = 0
        conn = psycopg2.connect(**DB_config)
        with conn.cursor() as cursor:
            cursor.execute("select url, topic from url_to_topic")
            data_to_parse = cursor.fetchall()
        with conn.cursor() as cursor:
            cursor.execute("select word, url from word_to_url")
            ALL_WORDS_AND_LINKS = cursor.fetchall()
        max_ = len(data_to_parse)
        for url, text in data_to_parse:
            i += 1
            for word in set(re.split(r"\W", text.lower())):
                if word in ("", " "):
                    continue
                if (word, url) in ALL_WORDS_AND_LINKS or (word, url) in WORDS_IN_SESSION:
                    continue
                save_to_db(conn, url, word)
                # thread = Thread(target=save_to_db, args=(conn, url, word, index))
                # thread.setDaemon(True)
                # thread.start()
                WORDS_IN_SESSION.append((word, url))
            print("{0} from {1} done! url: {2}".format(i, max_, url))
    except Exception as err:
        logging.error(err)
    finally:
        if conn is not None:
            conn.close()


if __name__ == "__main__":
    logging.basicConfig(filename="task_2.log", level=logging.DEBUG, filemode="w")
    start = time.perf_counter()
    main()
    print("Time spent: {0:6.4f} sec".format(time.perf_counter() - start))