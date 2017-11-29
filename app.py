import csv

from sqlalchemy import Column, Integer, Float, DateTime, Date, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from config import *

Base = declarative_base()

class Netvalue(Base):
    __tablename__ = 'netvalue'

    id = Column(Integer, primary_key=True)
    value = Column(Float)
    record_time = Column(DateTime)
    date = Column(Date)

    def __str__(self):
        return "netvalue #{}: value on date {} is {}, recorded at {}\n".format(self.id, self.date, self.value, self.record_time)


def connect_database():
    url = "{dialect}+{driver}://{username}:{password}@{host}/{db}?charset=utf8"
    print(url)
    url = url.format(
        dialect='mysql', driver='mysqldb',
        username=DB_USER, password=DB_PASS,
        host=DB_HOST, db=DB_NAME
    )
    engine = create_engine(url)
    Session = sessionmaker(bind=engine)
    return engine, Session


def validate(item):
    if item.id <= 10 or item.id > 20:
        return False

    if item.value < 1:
        return False

    return True


def extract_data_from_database(Session):
    session = Session()
    queryset = session.query(Netvalue).filter(Netvalue.id>10)
    data = []
    for item in queryset:
        if validate(item):
            data.append(item)
    return data


def save_data(data, file):
    with open(file, 'w') as f:
        writer = csv.writer(f)
        for item in data:
            writer.writerow([item.id, item.value])


def entry():
    engine, Session = connect_database()
    data = extract_data_from_database(Session)
    save_data(data, 'data.csv')


if __name__ == '__main__':
    entry()
