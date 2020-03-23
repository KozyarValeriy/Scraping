-- Таблица для хранения полных текстов статей
CREATE TABLE url_to_topic (
    url             text,
    CONSTRAINT url_to_topic_pkey PRIMARY KEY(url),
    topic           text NOT NULL,
    update_date     TIME WITHOUT TIME ZONE NOT NULL
);

-- Табица для хранения ссылок вида: слово -> ссылка на статью
CREATE TABLE word_to_url (
    word            text,
    url             text,
    CONSTRAINT pk PRIMARY KEY(word, url)
);
