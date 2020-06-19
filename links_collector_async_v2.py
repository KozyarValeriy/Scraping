"""
    Класс для работы с ссылками.
    Сохраняет статьи в базу данных, а также строит обратный индекс для слов.
"""

import logging
import time
import re
from urllib.parse import urljoin, urlsplit
import asyncio
# import pdb

from bs4 import BeautifulSoup
import asyncpg
import aiohttp

from config import URL, DB_config_async

# форматы, не являющиеся страницами
NOT_PAGE = (".png", ".pdf", ".jpeg", ".bmp")
# Невидимые элементы
INVISIBLE_ELEMENTS = ('style', 'script', 'head', 'title')
TOPIC_TABLE = "url_to_topic_async"
WORD_TABLE = "word_to_url_async"

LINKS = set()
WORDS_AND_URLS = set()


logging.basicConfig(level=logging.ERROR,
                    format="%(asctime)s\t%(name)s\t%(levelname)s\t%(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S")

log = logging.getLogger("Scraping_async")

handler = logging.FileHandler("collector_async.log")
handler.setLevel(logging.INFO)
handler.setFormatter(logging.Formatter(fmt="%(asctime)s\t%(name)s\t%(levelname)s\t%(message)s",
                                       datefmt="%Y-%m-%d %H:%M:%S"))
log.addHandler(handler)


async def collect_all_links(url: str):
    """ Функция для инициализации обхода сайта """
    global LINKS, WORDS_AND_URLS
    try:
        # получаем соединения с БД
        pool = await asyncpg.create_pool(**DB_config_async)
        # conn = await asyncpg.connect(**DB_config_async)
        async with pool.acquire() as conn:
            # Запрос к БД, чтобы узнать, какие страницы уже сохранены в БД
            links_in_db = await conn.fetch(f"select url from {TOPIC_TABLE}")
            LINKS = set(link[0] for link in links_in_db)
            words_and_urls_in_db = await conn.fetch(f"select word, url from {WORD_TABLE}")
            WORDS_AND_URLS = set((record['word'], record['url']) for record in words_and_urls_in_db)
        log.info("Success getting start position")
        # вызываем рекурсивную функцию
        # async with aiohttp.ClientSession() as session:
        await _collect_all_links(url, pool)
    except Exception as err:
        log.error(f'Error appeared "{err}" in "collect_all_links"')


async def fetch(session, url):
    async with session.get(url) as response:
        # print(response.url)
        return str(response.url), await response.text()


async def _collect_all_links(url: str, pool, links_in_session: set = set()):
    """ Рекурсивная функция для обхода страниц сайта

    :param url: сылка для парсинга,
    :param pool: пул соединений с базой данных,
    :param links_in_session: ссылки, которые обрабатывлись в текущей сессии
            (задается как изменяемый тип в определении функции для сохранения результата между вызовами)
    """
    global LINKS, WORDS_AND_URLS
    if url in links_in_session:
        return
    # Добавляем эту ссылку в пройденные в этой сессии
    links_in_session.add(url)
    log.info(f"Getting URL '{url}' in _collect_all_links")
    async with aiohttp.ClientSession() as session:
        url, page = await fetch(session, url)
        # Добавляем ссылку, по которой удалось получить страницу, чтобы исключить неправильный протокол
        links_in_session.add(url)
    soup = BeautifulSoup(page, 'lxml')
    # Если страницу по этому url еще нет в БД, то записываем ее в БД
    if url not in LINKS:
        text = ' '.join([s for s in soup.strings if s.parent.name not in INVISIBLE_ELEMENTS])
        await _prepare_text(url, text, pool)
    # Coroutines, которы надо выполнить
    tasks = list()
    # Обходим все теги <a> на странице
    for link in set(soup.findAll("a")):
        try:
            text_link = str(link.get("href"))
            # если нет ссылки, то пропускаем
            if text_link is None:
                continue
            # отбрасываем якорь
            current_url = urljoin(URL, text_link).split("#")[0]
            # Если это не ссылка на этот же сайт или это не страница, то пропускаем
            if urlsplit(current_url).netloc != urlsplit(URL).netloc or current_url.endswith(NOT_PAGE):
                continue
            # вызываем функцию для следующего url
            tasks.append(_collect_all_links(current_url, pool))
        except Exception as err:
            log.error(f'Error appeared "{err}" in "_collect_all_links" on URL: {url} on scrapping page')
    await asyncio.gather(*tasks)


async def _prepare_text(url: str, text: str, pool):
    """ Функция для подготовки и записи текста в БД, а также
        построения обратного ключа.

    :param url: ссылка на текущую статью
    :param text: текст статьи,
    :param pool: пул соединений с базой данных
    """
    global LINKS, WORDS_AND_URLS
    # подготовка текста для записи в БД
    text = text.strip().replace('\'', '"')
    # заменяем повторяющиеся переносы и пробелы на один символ
    text = re.sub(r"\n+", r"\n", text)
    text = re.sub(r" +", r" ", text).strip()
    # tasks = list()
    async with pool.acquire() as conn:
        async with conn.transaction():
            try:
                # сохраняем топик в БД
                # tasks.add(conn.execute(f"insert into {TOPIC_TABLE} values($1, $2)", url, text))
                LINKS.add(url)
                await conn.execute(f"insert into {TOPIC_TABLE} values($1, $2)", url, text)
                log.info(f"URL '{url}' was successfully added.")
                # бходим все слова для построения обратного индекса
                for word in set(re.split(r"\W", text.lower())):
                    if word in ("", " "):
                        continue
                    if (word, url) in WORDS_AND_URLS:
                        continue
                    WORDS_AND_URLS.add((word, url))
                    # tasks.append(conn.execute(f"insert into {WORD_TABLE} values($1, $2)", word, url))
                    await conn.execute(f"insert into {WORD_TABLE} values($1, $2)", word, url)
                log.info(f"All words in URL '{url}' was successfully added.")
            except Exception as err:
                log.error(f'Error appeared "{err}" in "_prepare_text" on URL: {url}')
            # await asyncio.gather(*tasks)


if __name__ == "__main__":
    start = time.perf_counter()
    asyncio.run(collect_all_links(URL))
    print("Time spent: {0:6.4f} sec".format(time.perf_counter() - start))
