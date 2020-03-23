"""
    Класс для работы с ссылками.
    Сохраняет статьи в базу данных, а также строит обратный индекс для слов.
"""

from threading import Thread, Lock
import logging
import time
import re

from bs4 import BeautifulSoup
import requests

from config import URL, DB_config
from SingletonDB import SingletonDB

logging.basicConfig(filename="Collector.log", level=logging.INFO, filemode="w")


class Collector:
    """ Класс для сбора всех страниц с сайта """
    # форматы, не являющиеся страницами
    NOT_PAGE = (".png", ".pdf", ".jpeg", ".bmp")

    def __init__(self, url: str, db_config: dict, topic_table: str, word_table: str):
        """ Инициализация экземпляра класса

        :param url: ссылка на страницу сайта, который необходимо обойти
        :param db_config: словарь с конфигурациями для подключения к БД
        :param topic_table: название таблицы, в которой хранится: ссылка -> весь текст сайта
        :param word_table: название таблицы, в которой хранится: слово -> ссылка на статью, где есть это слово
        """
        self.url = url
        self.db_config = db_config
        self.topic_table = topic_table
        self.word_table = word_table
        # список тредов для закртия их в конце
        self._all_thread = []
        # ссылки, которые обходились в текущей сессии
        self._links_in_session = set()
        # множество уже сохраненных в БД ссылок и статей
        self._all_words_and_links = set()
        # множество уже сохраненных в БД ссылок и статей
        self._all_links = set()
        # мьютекс
        self._lock = Lock()

    def collect_all_links(self):
        """ Метод для инициализации обхода сайта """
        conn = None
        try:
            # получаем соединения с БД
            conn = SingletonDB(self.db_config)
            # Запрос к БД, чтобы узнать, какие страницы уже сохранены в БД
            with conn.connect_to_DB.cursor() as cursor:
                cursor.execute(f"select url from {self.topic_table}")
                links = cursor.fetchall()
                # сохраняем во множество, так как поиск по множеству - O(1)
                self._all_links = set(link[0] for link in links)
            # Запрос к БД, чтобы узнать, какие слова уже есть в БД
            with conn.connect_to_DB.cursor() as cursor:
                cursor.execute(f"select word, url from {self.word_table}")
                self._all_words_and_links = set(cursor.fetchall())
            # вызываем рекурсивную функцию
            self._collect_all_links(self.url)
        except Exception as err:
            logging.error(err)
        finally:
            if conn is not None:
                # если было соеднинение с БД, то ждем завершения всех потоков и закрываем соединение
                for thread in self._all_thread:
                    thread.join()
                conn.connect_to_DB.close()
                # обнуляем коллекции, характеризующие текущую сессию
                self._all_thread = []
                self._links_in_session = set()

    def _collect_all_links(self, url):
        """ Рекурсивный метод для обхода страниц сайта """
        if url in self._links_in_session:
            return
        page = requests.get(url)
        soup = BeautifulSoup(page.text, 'lxml')
        # Если страницу по этому url еще нет в БД, то записываем ее в БД
        if url not in self._all_links:
            self._all_thread.append(Thread(target=self._prepare_text, args=(url, soup.find('body').get_text())))
            self._all_thread[-1].start()
        # Добавляем эту ссылку в пройденные в этой сессии
        self._links_in_session.add(url)
        # Обходим все теги <a> на странице
        for link in set(soup.findAll("a")):
            try:
                text_link = str(link["href"])
                # Если это не ссылка на этот же сайт или это не страница, то пропускаем
                if not text_link.startswith(('/', self.url)) or text_link.endswith(self.NOT_PAGE):
                    continue
                # Для относительных ссылок добавляем в начало главный url
                text_link = text_link if text_link.startswith(self.url) else "".join([self.url, text_link])
                text_link = text_link.rstrip('/')
                # вызываем функцию для следующего url
                self._collect_all_links(text_link)
            except Exception as err:
                logging.error(err)

    def _prepare_text(self, url: str, text: str):
        """ Метод для подготовки и записи текста в БД, а также
            построения обратного ключа.

            :param url: ссылка на текущую статью
            :param text: текст статьи.
        """
        # подготовка текста для записи в БД
        text = text.strip().replace('\'', '"')
        # удаляем часть скрипка, который в теле текства
        text = re.sub(r"\s*window\.dataLayer([^;]*;){4}", "\n", text)
        text = re.sub(r"\s*try{([^}]*}){3}", "\n", text)
        text = re.sub(r"\s*\(function([^;]*;){10}", "\n", text)
        # заменяем повторяющиеся переносы и пробелы на один символ
        text = re.sub(r"\n+", r"\n", text)
        text = re.sub(r" +", r" ", text).strip()
        # формирование запроса
        command = f"insert into {self.topic_table} values('{url}', '{text}')"
        conn = None
        try:
            # ставим мьютекс
            self._lock.acquire()
            # получаем конект к БД
            conn = SingletonDB(self.db_config)
            # сохраняем топик в БД
            self.save_to_db(command)
            self._all_links.add(url)
            # бходим все слова для построения обратного индекса
            for word in set(re.split(r"\W", text)):
                if word in ("", " "):
                    continue
                if (word, url) in self._all_words_and_links:
                    continue
                command = f"insert into {self.word_table} values('{word}', '{url}')"
                self.save_to_db(command)
                self._all_words_and_links.add((word, url))
            # делаем коммит в самом конце, чтобы избежать ошибок
            conn.connect_to_DB.commit()
        except Exception as err:
            logging.error(err)
            if conn is not None:
                # если была ошибка, то откатываемся
                conn.connect_to_DB.rollback()
        finally:
            self._lock.release()

    def save_to_db(self, action):
        """ Метод для выполнения строки action в БД """
        conn = SingletonDB(self.db_config)
        with conn.connect_to_DB.cursor() as cursor:
            cursor.execute(action)


if __name__ == "__main__":
    start = time.perf_counter()
    act = Collector(URL, DB_config, "url_to_topic_test", "word_to_url_test")
    act.collect_all_links()
    print("Time spent: {0:6.4f} sec".format(time.perf_counter() - start))
