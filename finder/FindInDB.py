import logging
import re

import psycopg2
from config import DB_config

# диапазон кол-ва символов в выдаче результата
RANGE = 500

logging.basicConfig(filename="task_3.log", level=logging.DEBUG, filemode="w")


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
            cursor.execute(f"select url from word_to_url where word = '{word.lower()}'")
            all_links = cursor.fetchall()
        with conn.cursor() as cursor:
            for link in all_links:
                try:
                    cursor.execute(f"select topic from url_to_topic where url = '{link[0]}'")
                    text = cursor.fetchall()[0][0]
                    # заменяем все символы переноса строки и табуляции на пробелы
                    text = re.sub(r"[\n \r\t]", r' ', text)
                    # позиция слова в тексте
                    pos = text.lower().find(word)
                    # получаем границы диапазона в зависимости от расположения слова в тексте
                    if pos < RANGE // 2:
                        # если слово стоит от начала ближе, чем половина диапазона
                        start_step = 0
                        stop_step = text.find(" ", RANGE)
                    elif pos + len(word) > len(text) - RANGE // 2:
                        # если слово стоит от конца ближе, чем половина диапазона
                        start_step = text.rfind(" ", 0, len(text) - RANGE) + 1
                        stop_step = len(text)
                    else:
                        # если слово в середине текста
                        start_step = text.rfind(" ", 0, pos - RANGE // 2) + 1
                        stop_step = text.find(" ", pos + len(word) + RANGE // 2)
                    # Если поиск был неудачный (вернул -1), то назначаем крайние границы
                    start_step = start_step if start_step > 0 else 0
                    stop_step = stop_step if stop_step > 0 else len(text)
                    # Получаем текст до и после искомого слова в соответсвиии с диапазоном
                    body_first = text[start_step: pos]
                    body_last = text[pos + len(word) + 1: stop_step]
                    # если не начало или не конец текста, то добавляем ...
                    if start_step != 0:
                        body_first = "..." + body_first
                    if stop_step != len(text):
                        body_last = body_last + "..."
                    result.append({"path": link[0],
                                   "body_first": body_first,
                                   "body_last": body_last,
                                   "word": text[pos: pos + len(word)]})
                except Exception as err:
                    print(err)
                    logging.error(err)
    except Exception as err:
        print(err)
        logging.error(err)
    finally:
        if conn is not None:
            conn.close()
    return result


if __name__ == "__main__":
    data = get_topics('test')
    print(len(data))
    print(data)
