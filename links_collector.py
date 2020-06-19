"""
    Класс для работы с ссылками.
    Сохраняет статьи в базу данных, а также строит обратный индекс для слов.
"""

import logging
import time
import re
from urllib.parse import urljoin, urlsplit

from bs4 import BeautifulSoup
import requests
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

from config import URL, DB_config
from finder.models import URLToTopic, WordToURL


logging.basicConfig(level=logging.ERROR,
                    format="%(asctime)s\t%(name)s\t%(levelname)s\t%(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S")

log = logging.getLogger("links_collector")

handler = logging.FileHandler("collector.log")
handler.setLevel(logging.INFO)
handler.setFormatter(logging.Formatter(fmt="%(asctime)s\t%(name)s\t%(levelname)s\t%(message)s",
                                       datefmt="%Y-%m-%d %H:%M:%S"))
log.addHandler(handler)


class Collector:
    """ Класс для сбора всех страниц с сайта """
    # форматы, не являющиеся страницами
    NOT_PAGE = (".png", ".pdf", ".jpeg", ".bmp")
    # Невидимые элементы
    INVISIBLE_ELEMENTS = ('style', 'script', 'head', 'title')

    def __init__(self, url: str, session: Session):
        """ Инициализация экземпляра класса

        :param url: ссылка на страницу сайта, который необходимо обойти
        :param session: сессия для работы с БД
        """
        self.url = url
        self.domain = urlsplit(url).netloc
        self.session = session
        # ссылки, которые обходились в текущей сессии
        self._links_in_session = set()
        # множество уже сохраненных в БД ссылок и статей
        self._all_words_and_links = set()
        # множество уже сохраненных в БД ссылок и статей
        self._all_links = set()

    def collect_all_links(self):
        """ Метод для инициализации обхода сайта """
        try:
            # Запрос к БД, чтобы узнать, какие страницы уже сохранены в БД
            query_all_url = self.session.query(URLToTopic.url)
            links = self.session.execute(query_all_url).fetchall()
            self._all_links = set(link[0] for link in links)
            # Запрос к БД, чтобы узнать, какие слова уже есть в БД
            query_all_words = self.session.query(WordToURL.word, WordToURL.url)
            all_words_and_links = self.session.execute(query_all_words).fetchall()
            self._all_words_and_links = set([tuple(el) for el in all_words_and_links])
            log.info("Success getting start position in database")
            # вызываем рекурсивную функцию
            self._collect_all_links(self.url)
        except Exception as err:
            log.error(f'Error appeared "{err}" in "collect_all_links"')
        finally:
            # обнуляем коллекции, характеризующие текущую сессию
            self._links_in_session = set()
            # self.session.commit()

    def _collect_all_links(self, url):
        """ Рекурсивный метод для обхода страниц сайта """
        if url in self._links_in_session:
            return
        page = requests.get(url)
        soup = BeautifulSoup(page.text, 'lxml')
        # Если страницу по этому url еще нет в БД, то записываем ее в БД
        if url not in self._all_links:
            text = ' '.join([s for s in soup.strings if s.parent.name not in self.INVISIBLE_ELEMENTS])
            try:
                self._save_to_db(url, text)
                self._all_links.add(url)
                # self.session.begin_nested()
            except Exception as err:
                log.error(f'Error appeared "{err}" in "_collect_all_links" on URL: {url}')
                self.session.rollback()
        # Добавляем эту ссылку в пройденные в этой сессии
        self._links_in_session.add(url)
        print(len(set(soup.findAll("a"))), url)
        # Обходим все теги <a> на странице
        for link in set(soup.findAll("a")):
            try:
                text_link = str(link.get("href"))
                # если нет ссылки или если это якорь, то пропускаем
                if text_link is None or text_link.startswith("#"):
                    continue
                current_url = urljoin(self.url, text_link)
                # Если это не ссылка на этот же сайт или это не страница, то пропускаем
                if urlsplit(current_url).netloc != self.domain or current_url.endswith(self.NOT_PAGE):
                    continue
                # вызываем функцию для следующего url
                self._collect_all_links(current_url)
            except Exception as err:
                log.error(f'Error appeared "{err}" in "_collect_all_links" on URL: {url} on scrapping page')

    def _save_to_db(self, url: str, text: str):
        """ Метод для подготовки и записи текста в БД, а также
            построения обратного ключа.

            :param url: ссылка на текущую статью
            :param text: текст статьи.
        """
        # создаем savepoint, чтобы откатиться, если будут какие то ошибки
        self.session.begin_nested()
        current_topic = URLToTopic(url=url, topic=text)
        current_topic.clean_topic()
        self.session.add(current_topic)
        self.session.commit()
        log.info(f"URL '{url}' was successfully added.")
        try:
            # бходим все слова для построения обратного индекса
            for word in set(re.split(r"\W", current_topic.topic.lower())):
                if word in ("", " "):
                    continue
                if (word, url) in self._all_words_and_links:
                    continue
                current_word = WordToURL(word=word, url=url)
                self.session.add(current_word)
                self._all_words_and_links.add((word, url))
            self.session.commit()
            log.info(f"All words in URL '{url}' was successfully added.")
        except Exception as err:
            log.error(f'Error appeared "{err}" in "_save_to_db" on URL: {url}')


if __name__ == "__main__":
    start = time.perf_counter()
    engine = create_engine(f"postgresql://{DB_config['user']}:{DB_config['password']}"
                           f"@{DB_config['host']}/{DB_config['dbname']}",
                           echo=False)
    Session = sessionmaker(bind=engine)
    current_session = Session()

    act = Collector(URL, current_session)
    act.collect_all_links()
    print("Time spent: {0:6.4f} sec".format(time.perf_counter() - start))
