#! /usr/bin/python

# Load chart from Second measure export
# ACloudGuru,https://acloud.guru/,2015-10-01,491
# this template
# http://stackoverflow.com/questions/31394998/using-sqlalchemy-to-load-csv-file-into-a-database
# how to make a string column
# http://docs.sqlalchemy.org/en/latest/core/metadata.html#creating-and-dropping-database-tables
import csv
import ConfigParser
import traceback
import re
from time import time
from datetime import datetime
from sqlalchemy import Boolean, Column, Integer, Float, DateTime, String, BigInteger, ForeignKey, Index
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sp_util import format_string, format_date, format_domain

Base = declarative_base()

class SmMonthlyRevenue(Base):
    __tablename__ = 'sm_monthly_revenue'
    id = Column(Integer, primary_key=True, nullable=False)
    name = Column(String(255))
    domain = Column(String(255))
    month = Column(DateTime())
    observed_sales = Column(BigInteger())
    __table_args__ = (Index('name', 'name'),
                      Index('month', 'month'),
                      Index('domain', 'domain'),
                      Index('nd', 'name', 'domain'))

def build_object(data):
    return SmMonthlyRevenue(**{
        'name':data[0],
        'domain':format_domain(data[1]),
        'month':data[2],
        'observed_sales':data[3]
    })

def load_from_sm_csv(csvfile, engine):
    Base.metadata.create_all(engine)

    #Create the session
    session = sessionmaker()
    session.configure(bind=engine)
    s = session()

    print 'loading file'
    rounds = csv.reader(csvfile)
    data = list(rounds)[1:]
    print 'loaded file'

    #print 'loading to database'
    for i in range(0, len(data)):
    #for i in range(000, 10):
        try:
            #if (data[i][0] == 'Wordpress'):
            #    print str(data[i])
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
    Base.metadata.create_all(engine)

    connection = engine.connect()
    result = connection.execute('DROP TABLE IF EXISTS sm_monthly_revenue')
    connection.close()

    with open('../../1old_ds5000/data/sales_monthly_3.csv', 'rU') as csvfile:
        load_from_sm_csv(csvfile=csvfile, engine=engine)

    print "Time elapsed: " + str(time() - t) + " s." #0.091s
