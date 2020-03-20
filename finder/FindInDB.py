import logging
import re

import psycopg2
from config import DB_config

# диапазон кол-ва символов в выдаче результата
RANGE = 500


def get_topics(word: str) -> list:
    """ Функция для получения всех статей, содержащих слово

     :param word: слово для поиска,
     :return result: список словарей вида
                {"path": ссылка на статью, содержащую слово,
                 "body_first": половина диапазона (или меньше) до искомого слова,
                 "body_last": половина диапазона (или меньше) после искомого слова,
                 "word": слово}
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
                # заменяем все символы переноса строки и табуляции на пробелы
                text = re.sub(r"[\n \r\t]", r' ', text)
                # позиция слова в тексте
                pos = text.find(word)
                # получаем границы диапазона в зависимости от расположения слова в тексте
                if pos < RANGE // 2:
                    # если слово стоит от начала ближе, чем половина диапазона
                    start_step = 0
                    stop_step = text.find(" ", RANGE)
                elif pos + len(word) > len(text) - RANGE // 2:
                    # если слово стоит от конца ближе, чем половина диапазона
                    start_step = text.rfind(" ", 0, len(text) - RANGE)
                    stop_step = len(text)
                else:
                    # если слово в середине текста
                    start_step = text.rfind(" ", 0, pos - RANGE // 2)
                    stop_step = text.find(" ", pos + len(word) + RANGE // 2)

                start_step = start_step if start_step > 0 else 0
                stop_step = stop_step if stop_step > 0 else len(text)

                # text = re.split(r"[\n \r\t]", text)
                # if pos < RANGE // 2:
                #     start_step = 0
                #     stop_step = RANGE
                # elif len(text) - pos < RANGE // 2:
                #     start_step = len(text) - RANGE
                #     stop_step = len(text)
                # else:
                #     stop_step = pos + RANGE // 2
                #     start_step = pos - RANGE // 2
                # body_first = " ".join(text[start_step: pos])
                # body_last = " ".join(text[pos + 1: stop_step])

                body_first = text[start_step: pos]
                body_last = text[pos + len(word) + 1: stop_step]
                if start_step != 0:
                    body_first = "..." + body_first
                if stop_step != len(text):
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
