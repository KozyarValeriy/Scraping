"""
    Роутер для поиска информации на странице.
"""
from threading import Thread
import pprint

from flask import render_template
from flask import request
from flask import Flask, flash
from flask import jsonify
from flask_mail import Mail, Message
# from flask import flash
from flask_script import Manager

from config import URL
from finder import FindInDB
from finder.forms import SearchForm


app = Flask(__name__)
app.config['SECRET_KEY'] = 'a really really really really long secret key'
app.config['JSON_AS_ASCII'] = False

app.config['MAIL_SERVER'] = 'smtp.gmail.com'
app.config['MAIL_PORT'] = 587
app.config['MAIL_USE_TLS'] = True
app.config['MAIL_USERNAME'] = 'monqpepers@gmail.com'  # введите свой адрес электронной почты здесь
app.config['MAIL_DEFAULT_SENDER'] = 'monqpepers@gmail.com'  # и здесь
app.config['MAIL_PASSWORD'] = 'yxeejjyndwrsihnt'  # введите пароль

mail = Mail(app)

manage = Manager(app)
counter = 0


def async_send_mail(msg):
    with app.app_context():
        mail.send(msg)


def send_mail(subject, recipient, body):
    msg = Message(subject, sender=app.config['MAIL_DEFAULT_SENDER'], recipients=[recipient])
    msg.body = body
    thread = Thread(target=async_send_mail, args=(msg,))
    thread.start()
    return thread


@app.route('/', methods=["POST", "GET"])
def main_page():
    query = ""
    form = SearchForm()
    if request.method == "POST":
        query = request.form.get("search").strip()
        form = SearchForm(request.form)
    # flash(f"Получил данные {query}", "success")
    if query == "":
        return render_template("main.html", search_url=URL, result=False, form=form)
    else:
        urls = FindInDB.Finder.get_match(query)
        # pprint.pprint(urls)
        return render_template("main.html", search_url=URL, result=True, urls=urls, count=len(urls), form=form)


@app.route('/api/v1/search_word/<string:word>')
def search_in_db(word: str):
    word = word.strip()
    urls = FindInDB.Finder.get_match(word)
    msg_body = f"There was request on word: '{word}'. Received {len(urls)} results."
    send_mail("Request", 'monqpepers@yandex.ru', msg_body)
    return jsonify(urls)


@app.route('/form', methods=["POST", "GET"])
def form_test():
    query = ""
    if request.method == "POST":
        query = request.form.get("search").strip()
        # query = None if query == "" else query.strip()
    if query == "":
        return render_template("main_old.html", search_url=URL, result=False)
    else:
        urls = FindInDB.Finder.get_match(query)
        return render_template("main_old.html", search_url=URL, result=True, urls=urls, count=len(urls), query=query)


# from finder.models import TestTable
# from sqlalchemy import create_engine
# from sqlalchemy.orm import sessionmaker
# from config import DB_config
# engine = create_engine(f"postgresql://{DB_config['user']}:{DB_config['password']}"
#                        f"@{DB_config['host']}/{DB_config['dbname']}",
#                        echo=False)
# Session = sessionmaker(bind=engine)
# session = Session()
#
#
# @app.route('/add', methods=["POST", "GET"])
# def form_add():
#     global counter
#     data = TestTable(word=str(counter), url='test')
#     counter += 1
#     session.add(data)
#     return f"Successfully added id = {counter - 1}"
#
#
# @app.route('/commit', methods=["POST", "GET"])
# def form_commit():
#     session.commit()
#     return "Successfully commit"


if __name__ == "__main__":
    app.run(debug=True)
    # manage.run()
