import re

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Text, ForeignKey, create_engine, Integer
from sqlalchemy.orm import sessionmaker
from marshmallow import Schema, fields

from config import DB_config


Base = declarative_base()


class TestTable(Base):
    """ Класс таблицы для хранения соединения вида: word <-> url """
    __tablename__ = "test_session"

    id = Column(Integer, primary_key=True)
    name = Column(Text)

    def __repr__(self):
        return f"<TestTable(id='{self.id}', name='{self.name}')>"


class TestSchema(Schema):
    id = fields.Integer()
    name = fields.String()


if __name__ == "__main__":
    engine = create_engine(f"postgresql://{DB_config['user']}:{DB_config['password']}"
                           f"@{DB_config['host']}/{DB_config['dbname']}",
                           echo=True)
    Base.metadata.create_all(engine)

    # test
    d = TestTable(name='first')
    r = TestTable(name='second')
    Session = sessionmaker(bind=engine)
    session = Session()
