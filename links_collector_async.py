"""
    Класс для работы с ссылками.
    Сохраняет статьи в базу данных, а также строит обратный индекс для слов.
"""

import logging
import time
import re
from urllib.parse import urljoin, urlsplit
import asyncio

from bs4 import BeautifulSoup
import asyncpg
import aiohttp

from config import URL, DB_config_async


logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s\t%(name)s\t%(levelname)s\t%(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S")

log = logging.getLogger("Scraping_async")

handler = logging.FileHandler("collector_async.log")
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

    def __init__(self, url: str, db_config: dict, topic_table: str, word_table: str):
        """ Инициализация экземпляра класса

        :param url: ссылка на страницу сайта, который необходимо обойти
        :param db_config: словарь с конфигурациями для подключения к БД
        :param topic_table: название таблицы, в которой хранится:
                                ссылка -> весь текст сайта
        :param word_table: название таблицы, в которой хранится:
                                слово -> ссылка на статью, где есть это слово
        """
        self.url = url
        self.domain = urlsplit(url).netloc
        self.db_config = db_config
        self.topic_table = topic_table
        self.word_table = word_table

        # ссылки, которые обходились в текущей сессии
        self._links_in_session = set()
        # множество уже сохраненных в БД ссылок и статей
        self._all_words_and_links = set()
        # множество уже сохраненных в БД ссылок и статей
        self._all_links = set()
        self._pool = None

    async def collect_all_links(self):
        """ Метод для инициализации обхода сайта """
        try:
            # получаем пулл соединений с БД
            self._pool = await asyncpg.create_pool(**self.db_config)
            # из пула соединений получаем свободный коннект
            async with self._pool.acquire() as conn:
                # получаем все сохраненные urls из базы данных
                links_in_db = await conn.fetch(f"select url from {self.topic_table}")
                self._all_links = set(link[0] for link in links_in_db)
                # получаем все сохраненные (word <-> url) из базы данных
                words_and_urls_in_db = await conn.fetch(f"select word, url from {self.word_table}")
                self._all_words_and_links = set((record['word'], record['url']) for record in words_and_urls_in_db)
            log.info("Successful getting data from db")
            # вызываем рекурсивную функцию
            await self._collect_all_links(self.url)
        except Exception as err:
            log.error(f'Error appeared "{err}" in "collect_all_links"')
        finally:
            log.info(f'Closing connection pool...')
            if self._pool is not None:
                await self._pool.close()
                # обнуляем коллекции, характеризующие текущую сессию
                self._links_in_session = set()

    async def _collect_all_links(self, url: str):
        """ Рекурсивный метод для обхода страниц сайта

        :param url: ссылка для парсинга
        """
        if url in self._links_in_session:
            return
        # Добавляем эту ссылку в пройденные в этой сессии
        self._links_in_session.add(url)
        log.info(f"URL '{url}' added to the current session")
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                # получаем url, по которому был успешный ответ и текст страницы
                url, page = str(response.url), await response.text()
            # Добавляем ссылку, по которой удалось получить страницу, чтобы исключить неправильный протокол
            self._links_in_session.add(url)
        soup = BeautifulSoup(page, 'lxml')
        # Если страницу по этому url еще нет в БД, то записываем ее в БД
        if url not in self._all_links:
            text = ' '.join([s for s in soup.strings if s.parent.name not in self.INVISIBLE_ELEMENTS])
            await self._prepare_text(url, text)
        # Coroutines, которы надо выполнить
        tasks = set()
        # Обходим все теги <a> на странице
        for link in set(soup.findAll("a")):
            try:
                text_link = str(link.get("href"))
                # если нет ссылки, то пропускаем
                if text_link is None:
                    continue
                # отбрасываем якорь
                current_url = urljoin(self.url, text_link).split("#")[0]
                # Если это не ссылка на этот же сайт или это не страница, то пропускаем
                if urlsplit(current_url).netloc != self.domain or current_url.endswith(self.NOT_PAGE):
                    continue
                # вызываем функцию для следующего url
                tasks.add(self._collect_all_links(current_url))
            except Exception as err:
                log.error(f'Error appeared "{err}" in "_collect_all_links" on URL: {url} on scrapping page')
        await asyncio.gather(*tasks)

    async def _prepare_text(self, url: str, text: str):
        """ Метод для подготовки и записи текста в БД, а также
            построения обратного ключа.

            :param url: ссылка на текущую статью
            :param text: текст статьи.
        """
        # подготовка текста для записи в БД
        text = text.strip().replace('\'', '"')
        # заменяем повторяющиеся переносы и пробелы на один символ
        text = re.sub(r"\n+", r"\n", text)
        text = re.sub(r" +", r" ", text).strip()
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                try:
                    # добавляем в сет всех ссылок текущую ссылку
                    self._all_links.add(url)
                    # сохраняем топик в БД
                    await conn.execute(f"insert into {self.topic_table} values($1, $2)", url, text)
                    log.info(f"URL '{url}' was successfully added.")
                    # обходим все слова для построения обратного индекса
                    word_to_save = set()
                    for word in set(re.split(r"\W", text.lower())):
                        if word in ("", " "):
                            continue
                        if (word, url) in self._all_words_and_links:
                            continue
                        self._all_words_and_links.add((word, url))
                        word_to_save.add((word, url))
                    await conn.executemany(f"insert into {self.word_table} values($1, $2)", word_to_save)
                    log.info(f"All words in URL '{url}' was successfully added.")
                except Exception as err:
                    log.error(f'Error appeared "{err}" in "_prepare_text" on URL: {url}')
                    conn.rollback()


if __name__ == "__main__":
    start = time.perf_counter()
    act = Collector(URL, DB_config_async, "url_to_topic_async", "word_to_url_async")
    asyncio.run(act.collect_all_links())
    print("Time spent: {0:6.4f} sec".format(time.perf_counter() - start))
