import logging
import re

import psycopg2
from config import DB_config


RANGE = 300


def get_topics(word: str) -> list:
    """ Функция для получения всех статей, содержащих слово

     :param word: слово для поиска,
     :return result: список словарей вида
                {"path": ссылка на статью, содержащую слово,


                }
    """
    result = []
    conn = None
    try:
        conn = psycopg2.connect(**DB_config)
        with conn.cursor() as cursor:
            cursor.execute(f"select url, position from word_to_url where word = '{word.lower()}'")
            all_links = cursor.fetchall()
        with conn.cursor() as cursor:
            for link, pos in all_links:
                cursor.execute(f"select topic from url_to_topic where url = '{link}'")
                text = cursor.fetchall()[0][0]
                text = re.split(r"[\n \r\t]", text)
                if pos < RANGE // 2:
                    start_step = 0
                    stop_step = RANGE
                elif len(text) - pos < RANGE // 2:
                    start_step = len(text) - RANGE
                    stop_step = len(text)
                else:
                    stop_step = pos + RANGE // 2
                    start_step = pos - RANGE // 2
                # print(start_step, stop_step, stop_step - start_step)
                # page = (" ".join(text[start_step: pos]) +
                #         f" <span id='word-match'>{word}</span> " +
                #         " ".join(text[pos + 1: stop_step]))
                body_first = " ".join(text[start_step: pos])
                body_last = " ".join(text[pos + 1: stop_step])

                if start_step != 0:
                    # page = "..." + page
                    body_first = "..." + body_first
                if stop_step != len(text):
                    # page = page + "..."
                    body_last = body_last + "..."
                result.append({"path": link, "body_first": body_first, "body_last": body_last, "word": word})
    except Exception as err:
        print(err)
    finally:
        if conn is not None:
            conn.close()

    return result


if __name__ == "__main__":
    data = get_topics('test')
    print(len(data))
    print(data)
