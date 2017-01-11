#! /usr/bin/python
# -*- coding: utf-8 -*-

# take the cb csv of companies (excel export) and turn into a database
# this template
# http://stackoverflow.com/questions/31394998/using-sqlalchemy-to-load-csv-file-into-a-database
# how to make a string column
# http://docs.sqlalchemy.org/en/latest/core/metadata.html#creating-and-dropping-database-tables
import csv
import ConfigParser
import traceback
import re
from datetime import datetime
from time import time
from sqlalchemy import Column, BigInteger, Integer, Float, DateTime, String, Index
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sp_util import format_string, format_number, format_date, format_cb_usd_amt, format_domain

def Load_Data(file_name):
    with open(file_name, 'rU') as csvfile:
        rounds = csv.reader(csvfile)
        return list(rounds)[1:]

Base = declarative_base()

class CbCompany(Base):
    __tablename__ = 'cb_companies'
    #tell SQLAlchemy the name of column and its attributes:
    id = Column(Integer, primary_key=True, nullable=False)
    company_name = Column(String(255))
    domain = Column(String(255))
    country_code = Column(String(255))
    state_code = Column(String(255))
    region = Column(String(255))
    city = Column(String(255))
    status = Column(String(255))
    short_description = Column(String(1023))
    category_list = Column(String(1023))
    category_group_list = Column(String(1023))
    funding_rounds = Column(Integer())
    funding_total_usd = Column(Float())
    founded_on = Column(DateTime)
    first_funding_on = Column(DateTime)
    last_funding_on = Column(DateTime)
    closed_on = Column(DateTime)
    employee_count = Column(String(255))
    cb_url = Column(String(255))
    __table_args__ = (Index('name', 'company_name'), Index('url', 'cb_url'), Index('domain', 'domain'))

def create_object(data):
    record = CbCompany(**{
        'company_name':format_string(data[0]),
        'domain':format_domain(data[2]),
        'country_code':data[4],
        'state_code':data[5],
        'region':format_string(data[6]),
        'city':format_string(data[7]),
        'status':data[10],
        'short_description':format_string(data[11], 1000),
        'category_list':data[12],
        'category_group_list':data[13],
        'funding_rounds':format_number(data[14]),
        'funding_total_usd':format_cb_usd_amt(data[15]),
        'founded_on':format_date(data[16]),
        'first_funding_on':format_date(data[17]),
        'last_funding_on':format_date(data[18]),
        'closed_on':format_date(data[19]),
        'employee_count':data[20],
        'cb_url':format_string(data[25]),
    })
    return record

def load_from_crunchbase(file_name):
    t = time()

    #Create the database
    print 'connecting to mysql database'
    config = ConfigParser.ConfigParser()
    config.read('properties.ini')
    engine = create_engine(config.get('properties', 'engine_string'))
    Base.metadata.create_all(engine)

    #Create the session
    session = sessionmaker()
    session.configure(bind=engine)
    s = session()

    print 'loading file'
    data = Load_Data(file_name)
    print 'loaded file'

    print 'loading to database'
    for i in range(0, len(data)):
    #for i in range(0, 5):
        try:
            last_funding_date = format_date(data[i][18])
            if not (last_funding_date is None) \
                and (data[i][4] == 'USA') \
                and (data[i][1] == 'company') \
                and (data[i][10] == 'operating' or data[i][10] == 'ipo'): # \
                # and (last_funding_date < datetime.strptime('2016-10-01', '%Y-%m-%d')) \
                # and (last_funding_date > datetime.strptime('2013-10-01', '%Y-%m-%d')):
                record = create_object(data[i])
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
    print "Time elapsed: " + str(time() - t) + " s." #0.091s

if __name__ == "__main__":
    load_from_crunchbase('data/organizations.csv')
