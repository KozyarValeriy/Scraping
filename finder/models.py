import re

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Text, ForeignKey, create_engine, Integer, CLOB

from config import DB_config


Base = declarative_base()


class URLToTopic(Base):
    """ Класс таблицы для хранения соединения вида: url <-> текст_статьи """
    __tablename__ = "url_to_topic_async"

    url = Column(Text, primary_key=True)
    topic = Column(Text, nullable=False)

    def __repr__(self):
        return f"<URLToTopic(url='{self.url}')>"

    def get_all_words(self) -> set:
        """ Метод для получения всех слов статьи в виде множества """
        result_text = set(re.split(r"\W", self.topic.lower()))
        if "" in result_text:
            result_text.remove("")
        if " " in result_text:
            result_text.remove(" ")
        return result_text

    def clean_topic(self) -> None:
        """ Метод для очистки текста статьи от повторений пробелов и переносов """
        self.topic = re.sub(r" *\n+ *", r"\n", self.topic)
        self.topic = re.sub(r"\n+", r"\n", self.topic)
        self.topic = re.sub(r" +", r" ", self.topic)
        self.topic = self.topic.strip()


class WordToURL(Base):
    """ Класс таблицы для хранения соединения вида: word <-> url """
    __tablename__ = "word_to_url_async"

    word = Column(Text, primary_key=True)
    # url = Column(Text, primary_key=True)
    url = Column(Text, ForeignKey(URLToTopic.url), primary_key=True)

    def __repr__(self):
        return f"<WordToURL(word='{self.word}', url='{self.url}')>"


class TestTable(Base):
    """ Класс таблицы для хранения соединения вида: word <-> url """
    __tablename__ = "test_session"

    id = Column(Integer, primary_key=True)
    name = Column(Text)

    def __repr__(self):
        return f"<TestTable(id='{self.id}', name='{self.name}')>"


if __name__ == "__main__":
    engine = create_engine(f"postgresql://{DB_config['user']}:{DB_config['password']}"
                           f"@{DB_config['host']}/{DB_config['dbname']}",
                           echo=True)
    Base.metadata.create_all(engine)

    # d = TestTable(name='first')
    # r = TestTable(name='second')
    # from sqlalchemy.orm import sessionmaker
    #
    # Session = sessionmaker(bind=engine)
    # session = Session()
