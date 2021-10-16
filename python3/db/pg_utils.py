import os
import sqlalchemy
from contextlib import contextmanager
from requests.structures import CaseInsensitiveDict
from sqlalchemy.orm import sessionmaker, scoped_session

from logging import getLogger
logger = getLogger(__name__)


def get_session(connect_url):
    session = get_scoped_session(connect_url)
    return session


@contextmanager
def get_scoped_session(url: str):
    engine = sqlalchemy.create_engine(url, echo=True)
    maker = sessionmaker(bind=engine, autocommit=True)
    session = scoped_session(maker)
    try:
        yield session
    except Exception as ex:
        session.rollback()
        raise ex
    finally:
        session.close()


def query_sql(sql, params=None, **kwargs):
    with get_session(os.environ.get('PG_CONN_URL')) as session:
        rows = session.execute(sql, params).fetchall()
    if kwargs.get('insensitive_dict') is not False:
        rows = [CaseInsensitiveDict(x) for x in rows]
    return rows


def test_connectivity(url):
    engine = sqlalchemy.create_engine(url, echo=True)
    maker = sessionmaker(bind=engine, autocommit=True)
    session = maker()
    session.execute('create table if not exists mytable (id INTEGER PRIMARY KEY, name varchar)')
    session.execute('select * from mytable limit 1')
    session.close()


def main():
    url = os.environ.get('PG_CONN_URL')
    test_connectivity(url)
    query_sql('select * from mytable limit 1', db_type='pg')
    print('OK.')


if __name__ == '__main__':
    main()
