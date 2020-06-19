import logging
import re
from typing import List

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from config import DB_config
from finder.models import URLToTopic, WordToURL

# диапазон кол-ва символов в выдаче результата
RANGE = 500

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s\t%(name)s\t%(levelname)s\t%(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger(__name__)
handler = logging.FileHandler("Finder.log")
handler.setLevel(logging.INFO)
handler.setFormatter(logging.Formatter(fmt="%(asctime)s\t%(name)s\t%(levelname)s\t%(message)s",
                                       datefmt="%Y-%m-%d %H:%M:%S"))
log.addHandler(handler)


class Finder:

    @staticmethod
    def get_match(word: str) -> List[dict]:
        """ Метод для получения всех статей, содержащих слово

        :param word: слово для поиска,
        :return result: список словарей вида
                   {"path": [],
                    "body_first": половина диапазона (или меньше) до искомого слова,
                    "body_last": половина диапазона (или меньше) после искомого слова,
                    "word": слово}
           """
        result = []
        try:
            # получаем соединение с БД и сессию
            engine = create_engine(f"postgresql://{DB_config['user']}:{DB_config['password']}"
                                   f"@{DB_config['host']}/{DB_config['dbname']}",
                                   echo=False)
            Session = sessionmaker(bind=engine)
            current_session = Session()
            # если в слове есть пробелы
            if word.count(" ") > 0:
                # Заменяем все двойные пробелы на одинарные
                word = re.sub(r' +', ' ', word).strip()
                # ищем в одной таблице операцией LIKE
                query = current_session \
                    .query(URLToTopic.url, URLToTopic.topic) \
                    .filter(URLToTopic.topic.ilike(f'%{word}%'))
            else:
                # если пробелов нет, ищем в по в таблице с обратным индексом
                # делаем join для получения текста
                query = current_session \
                    .query(WordToURL.url, URLToTopic.topic) \
                    .filter(WordToURL.word == f'{word.lower()}') \
                    .join(URLToTopic, URLToTopic.url == WordToURL.url)
            for link, text in query:
                try:
                    current_result = Finder.search_word_in_topic(word.lower(), text, RANGE)
                    current_result["path"] = Finder.split_url(link)
                    result.append(current_result)
                except Exception as err:
                    log.error(f'An error "{err}" occurred on "{word}" on "{link}" in loop')
            log.info(f'All results on query "{word}" were received')
        except Exception as err:
            log.error(f'An error "{err}" occurred on connection to DB')
        return result

    @staticmethod
    def split_url(url: str) -> list:
        """ Метод для разбиения ссылки на подссылки

        :param url: ссылка, которую надо разбить на подссылки
        :return: скписок из кортежей, в котором нулевой элемент - прямя ссылка,
                 а 1 элеметн - конечный элемент для ссылки.
        """
        first = []
        pos_protocol = url.find('//')
        pos_protocol = 0 if pos_protocol < 0 else pos_protocol + 2
        count_links = 0
        for pos in range(pos_protocol, len(url)):
            if url[pos] == '/':
                first.append(url[:pos])
                count_links += 1
        first.append(url)
        last = url.rsplit('/', count_links)
        result = list(zip(first, last))
        return result

    @staticmethod
    def search_word_in_topic(word: str, topic: str, max_number_of_char: int) -> dict:
        """ Метод для поиска слова в тексте

        :param word: слово для поиска,
        :param topic: текст, в котором ищем слово,
        :param max_number_of_char: кол-во символов в итогом тексте со словом,
        :return: словарь вида: {"body_first": `текст статьи до слова с учетом диапазона max_number_of_char`,
                                "body_last": `текст статьи после слова с учетом диапазона max_number_of_char`,
                                "word": `слово`}
        """
        result = dict()
        # заменяем все переносы и табуляции на пробелы
        topic = re.sub(r"[\n \r\t]", r' ', topic)
        # позиция слова в тексте
        match_word = re.search(rf'(?P<start>^|\W+){word}($|\W+)', topic.lower())
        pos = match_word.start() + len(match_word.group('start'))
        # получаем границы диапазона в зависимости от расположения слова в тексте
        if pos < max_number_of_char // 2:
            # если слово стоит от начала ближе, чем половина диапазона
            start_step = 0
            stop_step = topic.find(" ", max_number_of_char)
        elif pos + len(word) > len(topic) - max_number_of_char // 2:
            # если слово стоит от конца ближе, чем половина диапазона
            start_step = topic.rfind(" ", 0, len(topic) - max_number_of_char) + 1
            stop_step = len(topic)
        else:
            # если слово в середине текста
            start_step = topic.rfind(" ", 0, pos - max_number_of_char // 2) + 1
            stop_step = topic.find(" ", pos + len(word) + max_number_of_char // 2)
        # Если поиск был неудачный (вернул -1), то назначаем крайние границы
        start_step = start_step if start_step > 0 else 0
        stop_step = stop_step if stop_step > 0 else len(topic)
        # Получаем текст до и после искомого слова в соответсвиии с диапазоном
        body_first = topic[start_step: pos]
        body_last = topic[pos + len(word) + 1: stop_step]
        # если не начало или не конец текста, то добавляем "..."
        if start_step != 0:
            body_first = "..." + body_first
        if stop_step != len(topic):
            body_last = body_last + "..."
        # собираем результирующий словарь
        result["body_first"] = body_first
        result["body_last"] = body_last
        result["word"] = topic[pos: pos + len(word)]
        return result


if __name__ == "__main__":
    data = Finder.get_match('test')
    print(len(data))
    print(data)
