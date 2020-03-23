""" Класс для получения коннекта к БД """

import psycopg2


class SingletonDB:
    """ Класс для получения коннекта к БД
        Соединение всегда одно и тоже.

    >>> from config import DB_config
    >>> data1 = SingletonDB(DB_config)
    >>> data2 = SingletonDB(DB_config)
    >>> data1 is data2
    True
    """
    _instance = None  # Текущий экземпляр класса
    config = None  # конфигурации для подключения к БД

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, config):
        """ Метод инициализации экземпляра
            :param config: словарь, содержащий ключи: dbname, user, password, host.
        """
        if self.config is None:
            self.config = config
            self.connect_to_DB = psycopg2.connect(**self.config)


if __name__ == "__main__":
    import doctest
    doctest.testmod()
