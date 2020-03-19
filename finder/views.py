"""
    Роутер для поиска информации на странице.
"""

from finder import app_task_3
from flask import render_template
from flask import request

from config import URL
from finder import FindInDB


@app_task_3.route('/', methods=["POST", "GET"])
def main_page():
    query = None
    if request.method == "POST":
        query = request.form.get("search")
    query = None if query == "" else query
    # urls = [{"path": r"https://pythonworld.ru/", "body": "bla bla bla"},
    #         {"path": r"https://yandex.ru/", "body": "bla2 bla2 bla2"}]
    if query is None:
        return render_template("main.html", search_url=URL, result=False)
    else:
        urls = FindInDB.get_topics(query)
        return render_template("main.html", search_url=URL, result=True, urls=urls, count=len(urls), query=query)
