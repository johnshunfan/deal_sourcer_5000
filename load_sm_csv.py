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
from sp_util import format_string, format_number, format_date, format_domain

def Load_Data(file_name):
    with open(file_name, 'rU') as csvfile:
        rounds = csv.reader(csvfile)
        return list(rounds)[1:]

Base = declarative_base()

class SmMonthlyRevenue(Base):
    #Tell SQLAlchemy what the table name is and if there's any table-specific arguments it should know about
    __tablename__ = 'sm_monthly_revenue'
    #tell SQLAlchemy the name of column and its attributes:
    id = Column(Integer, primary_key=True, nullable=False)
    name = Column(String(255))
    domain = Column(String(255))
    month = Column(DateTime())
    observed_sales = Column(BigInteger())
    __table_args__ = (Index('name', 'name'), Index('month', 'month'), Index('domain', 'domain'), Index('nd', 'name', 'domain'))

def build_object(data):
    return SmMonthlyRevenue(**{
        'name':data[0],
        'domain':format_domain(data[1]),
        'month':data[2],
        'observed_sales':data[3]
    })

def load_from_sm_csv(file_name):
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
    print "Time elapsed: " + str(time() - t) + " s." #0.091s

if __name__ == "__main__":
    load_from_sm_csv('data/sales_monthly.csv')
