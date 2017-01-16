#! /usr/bin/python

# Load Rounds from crunchbase CSV funded rounds . csv
# this template
# http://stackoverflow.com/questions/31394998/using-sqlalchemy-to-load-csv-file-into-a-database
# how to make a string column
# http://docs.sqlalchemy.org/en/latest/core/metadata.html#creating-and-dropping-database-tables
import csv
import traceback
import re
from time import time
from datetime import datetime

from sqlalchemy import Boolean, Column, Integer, Float, DateTime, String, BigInteger, ForeignKey, Index
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from sp_util import format_string, format_date, split_investors_cb, format_cb_usd_amt

def Load_Data(file_name):
    with open(file_name, 'rU') as csvfile:
        rounds = csv.reader(csvfile)
        return list(rounds)[1:]

Base = declarative_base()

class CbRound(Base):
    __tablename__ = 'cb_rounds'
    id = Column(Integer, primary_key=True, nullable=False)
    company_name = Column(String(255))
    country_code = Column(String(255))
    state_code = Column(String(255))
    region = Column(String(255))
    city = Column(String(255))
    company_category_list = Column(String(1023))
    funding_round_type = Column(String(255))
    funding_round_code = Column(String(255))
    announced_on = Column(DateTime)
    raised_amount_usd = Column(Float())
    raised_amount = Column(Float())
    raised_amount_currency_code = Column(String(255))
    target_amount_usd = Column(Float())
    target_amount = Column(Float())
    target_amount_currency_code = Column(String(255))
    post_money_valuation_usd = Column(Float())
    post_money_valuation = Column(Float())
    post_money_currency_code = Column(String(255))
    investor_count = Column(Integer())
    investor_names = Column(String(1023))
    cb_url = Column(String(255))
    cb_uuid = Column(String(255))
    funding_round_uuid = Column(String(255))
    __table_args__ = (Index('company_name', 'company_name'), Index('announced_on', 'announced_on'), Index('cu', 'cb_url'))

def build_object(data):
    return CbRound(**{
        'company_name': data[0],
        'country_code': data[1],
        'state_code': data[2],
        'region': data[3],
        'city': data[4],
        'company_category_list': data[5],
        'funding_round_type': data[6],
        'funding_round_code': data[7],
        'announced_on': format_date(data[8]),
        'raised_amount_usd': format_cb_usd_amt(data[9]),
        'raised_amount': format_cb_usd_amt(data[10]),
        'raised_amount_currency_code': data[11],
        'target_amount_usd': format_cb_usd_amt(data[12]),
        'target_amount': format_cb_usd_amt(data[13]),
        'target_amount_currency_code': data[14],
        'post_money_valuation_usd': format_cb_usd_amt(data[15]),
        'post_money_valuation': format_cb_usd_amt(data[16]),
        'post_money_currency_code': data[17],
        'investor_count': data[18],
        'investor_names': data[19],
        'cb_url': data[20],
        'cb_uuid': data[21],
        'funding_round_uuid': data[22]
    })

def load_from_crunchbase(csvfile, engine):
    Base.metadata.create_all(engine)

    #Create the session
    session = sessionmaker()
    session.configure(bind=engine)
    s = session()

    print 'loading file'
    rounds = [row for row in csv.reader(csvfile.read().splitlines())]
    # edit for different types of CSV
    data = list(rounds)[1:]
    print 'loaded file'

    print 'loading to database'
    for i in range(0, len(data)):
    #for i in range(000, 10):
        try:
            # only load if USA and round was within 3 years
            if (data[i][1] == 'USA') \
                and (format_date(data[i][8]) < datetime.strptime('2016-10-01', '%Y-%m-%d')) \
                and (format_date(data[i][8]) > datetime.strptime('2013-10-01', '%Y-%m-%d')):
                record = build_object(data[i])
                s.add(record) #Add all the records
        except:
            print 'error in: ' + str(i) + ', ' + data[i][0] + ':' + traceback.format_exc()
        if (i % 1000 == 0 or i == len(data) - 1):
            print 'index: ' + str(i)
            # if can't commit, then rollback
            try:
                s.commit()
            except:
                s.rollback() #Rollback the changes on error
                print 'Unexpected error on index ' + str(i) + ':' + traceback.format_exc()
                break

    s.close() #Close the connection

if __name__ == "__main__":
    t = time()

    #Create the database
    print 'connecting to mysql database'
    engine = create_engine('mysql://root@127.0.0.1/test3?charset=utf8mb4')

    connection = engine.connect()
    result = connection.execute('DROP TABLE IF EXISTS cb_rounds')
    connection.close()

    with open('../../1old_ds5000/data/funding_rounds.csv', 'rU') as csvfile:
        load_from_crunchbase(csvfile=csvfile, engine=engine)

    print "Time elapsed: " + str(time() - t) + " s." #0.091s
