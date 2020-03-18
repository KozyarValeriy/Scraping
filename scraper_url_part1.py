from bs4 import BeautifulSoup
import requests
from collections import deque

from config import URL

all_links = dict()
links_queue = deque()


def get_all_link():
    page = requests.get(URL)
    soup = BeautifulSoup(page.text, 'lxml')
    all_links[URL] = soup.get_text()
    for link in set(soup.findAll('a')):
        try:
            text_link = str(link['href'])
            if not text_link.startswith(('/', URL)):
                continue
            text_link = text_link if text_link.startswith(URL) else "".join([URL, text_link])
            text_link = text_link.rstrip('/')
            if text_link not in all_links:
                links_queue.append(text_link)
            print(text_link)
        except KeyError:
            pass
    print(all_links)


def main():
    # получение главной страницы
    page = requests.get(URL)
    soup = BeautifulSoup(page.text, 'lxml')
    all_links[URL] = soup.get_text()
    for link in set(soup.findAll('a')):
        try:
            text_link = str(link['href'])
            if not text_link.startswith(('/', URL)):
                continue
            text_link = text_link if text_link.startswith(URL) else "".join([URL, text_link])
            text_link = text_link.rstrip('/')
            if text_link not in all_links:
                links_queue.append(text_link)
            print(text_link)
        except KeyError:
            pass
    print(all_links)


main()
