from sqlalchemy import *
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import * 
from kafka import KafkaConsumer, consumer
import re
import time

#ETL Proses
def etl_proses():
    customer = KafkaConsumer(bootstrap_servers = ['localhost:9092'], api_version=(0,10))
    customer.subscribe('topic3')

    tweet = []
    max = 0
    for message in customer:
        if max <= 20:
            tweet.append(message)
        else:
            break
        max += 1
        print("Read ",max, " tweet")

    cleansing = [re.sub("b'","", str(i.value)) for i in tweet]
    kumpulan_tweet = [i.split(';') for i in cleansing]

    #schema
    engine = create_engine('mysql+mysqlconnector://root:@localhost/twittersony')
    Base = declarative_base()

    class Users (Base):
        __tablename__ = "tweet"
        index = Column(Integer, primary_key = True)
        id_user = Column(String(1000))
        cuitan = Column(String(1000))
        tanggal_cuit = Column(String(1000))

    Users.__table__.create(bind = engine, checkfirst = True)

    #Transform
    tweet = []
    index = 0
    for i in kumpulan_tweet:
        row = {}
        row['index'] = index
        row['id_user'] = i[0]
        row['cuitan'] = i[1]
        row['tanggal_cuit'] = i[2]
        tweet.append(row)

    #load
    Session = sessionmaker(bind = engine)
    session = Session()

    for cuitan in tweet:
        row = Users(**cuitan)
        session.add(row)

    session.commit()
    session.close()


def periodic_work(interval):
    while True:
        etl_proses()
        time.sleep(interval)

periodic_work(2*1)