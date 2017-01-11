#! /usr/bin/python

# transform companies and investors, load from pb and cb
# http://stackoverflow.com/questions/31394998/using-sqlalchemy-to-load-csv-file-into-a-database
# how to make a string column
# http://docs.sqlalchemy.org/en/latest/core/metadata.html#creating-and-dropping-database-tables
import csv
import ConfigParser
import traceback
import re
from time import time
from datetime import datetime
from sqlalchemy import Boolean, Column, Integer, Float, DateTime, String, BigInteger, ForeignKey, Index, or_
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sp_util import format_string, format_number, format_date, split_investors_pb, split_investors_cb
from load_pb_rounds import PbRound
from load_cb_rounds import CbRound
from load_cb_companies import CbCompany

Base = declarative_base()

class CompanyInvestor(Base):
    __tablename__ = 'investors'
    id = Column(Integer, primary_key=True, nullable=False)
    name = Column(String(255))
    domain = Column(String(255))
    investor = Column(String(255))
    online_profile_url = Column(String(255))
    __table_args__ = (Index('name', 'name'),
                      Index('domain', 'domain'),
                      Index('investor', 'investor'),
                      Index('url', 'online_profile_url'))

def build_from_cb(data, investor_name):
    return CompanyInvestor(**{
        'name': data.company_name,
        'domain': None,
        'investor': investor_name,
        'online_profile_url':data.cb_url,
    })

def build_from_pb(data, investor_name):
    return CompanyInvestor(**{
        'name': data.company_name,
        'domain': data.company_website,
        'investor': investor_name,
        'online_profile_url':data.pitchbook_link,
    })

def dedupe_investors(connection):
    '''Execute sql to delete duplicates in categories'''
    print 'de duplicating investors'
    try:
        result = connection.execute(
            '''
            DELETE a
            FROM investors a, investors b
            WHERE (a.name = b.name)
                AND (a.domain = b.domain)
                AND (a.investor = b.investor)
                AND (a.id < b.id)
            ''')
    except:
        print traceback.format_exc()
        return

def transform_to_investors(load_pb=False, load_cb=False, engine=None):
    Base.metadata.create_all(engine)

    #Create the session
    session = sessionmaker()
    session.configure(bind=engine)
    s = session()

    if load_cb:
        print 'loading cb rows'
        cb_data = s.query(CbRound).all()
        print 'loading cb to database'
        for i in range(0, len(cb_data)):
        #for i in range(000, 10):
            investor_array = split_investors_cb(cb_data[i].investor_names)
            try:
                for investor in investor_array:
                    if not investor == '':
                        record = build_from_cb(cb_data[i], investor)
                        s.add(record) #Add all the records
            except:
                print 'error in: ' + str(i) + ', ' + cb_data[i].company_name + ':' + traceback.format_exc()
            if (i % 1000 == 0 or i == len(cb_data) - 1):
                print 'index: ' + str(i)
                # if can't commit, then rollback
                try:
                    s.commit()
                except:
                    s.rollback() #Rollback the changes on error
                    print 'Unexpected error on index ' + str(i) + ':' + traceback.format_exc()
                    break

    if load_pb:
        print 'loading pb rows'
        pb_data = s.query(PbRound).all()
        print 'loading pb to database'
        for i in range(0, len(pb_data)):
        #for i in range(000, 10):
            investor_array = split_investors_pb(pb_data[i].investors)
            try:
                for investor in investor_array:
                    # check the database
                    if not investor == '':
                        # create a new one if it's not in the database
                        record = build_from_pb(pb_data[i], investor)
                        s.add(record) #Add all the records
            except:
                print 'error in: ' + str(i) + ', ' + pb_data[i].company_name + ':' + traceback.format_exc()
            if (i % 1000 == 0 or i == len(pb_data) - 1):
                print 'index: ' + str(i)
                # if can't commit, then rollback
                try:
                    s.commit()
                except:
                    s.rollback() #Rollback the changes on error
                    print 'Unexpected error on index ' + str(i) + ':' + traceback.format_exc()
                    break

    connection = engine.connect()
    result = connection.execute(
        '''
        UPDATE investors AS i
        JOIN cb_companies AS c
        ON i.online_profile_url = c.cb_url
        SET i.domain = c.domain
        WHERE i.domain IS NULL
        '''
    )

    s.close() #Close the connection

if __name__ == "__main__":
    t = time()

    #Create the database
    print 'connecting to mysql database'
    config = ConfigParser.ConfigParser()
    config.read('properties.ini')
    engine = create_engine(config.get('properties', 'engine_string'))

    transform_to_investors(load_pb=True, load_cb=True, engine=engine)

    print "Time elapsed: " + str(time() - t) + " s." #0.091s
