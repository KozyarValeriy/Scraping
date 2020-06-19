from flask_wtf import FlaskForm
from wtforms import StringField, SubmitField


class SearchForm(FlaskForm):
    """ Форма для поиска в базе данных """

    # поле для строки ввода
    search = StringField()
    # кнопка submit
    submit = SubmitField("Поиск")
