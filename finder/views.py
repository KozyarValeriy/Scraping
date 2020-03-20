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
    if query is None:
        return render_template("main.html", search_url=URL, result=False)
    else:
        urls = FindInDB.get_topics(query.lower())
        return render_template("main.html", search_url=URL, result=True, urls=urls, count=len(urls), query=query)
