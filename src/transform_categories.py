#! /usr/bin/python

# transform companies into categories
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
from sqlalchemy import Boolean, Column, Integer, Float, DateTime, String, BigInteger, ForeignKey, Index, or_
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sp_util import format_string, format_number, format_date
from load_cb_companies import CbCompany
from load_pb_rounds import PbRound

Base = declarative_base()

class CompanyCategory(Base):
    __tablename__ = 'categories'
    id = Column(Integer, primary_key=True, nullable=False)
    name = Column(String(255))
    domain = Column(String(255))
    category = Column(String(255))
    online_profile_url = Column(String(255))
    __table_args__ = (Index('name', 'name'),
                      Index('domain', 'domain'),
                      Index('category', 'category'),
                      Index('url', 'online_profile_url'))

def build_from_cb(data, category):
    return CompanyCategory(**{
        'name':data.company_name,
        'domain':data.domain,
        'category':category,
        'online_profile_url':data.cb_url,
    })

def build_from_pb(data, category):
    return CompanyCategory(**{
        'name':data.company_name,
        'domain':data.company_website,
        'category':category,
        'online_profile_url':data.pitchbook_link,
    })

def format_pb_categories(category_string):
    category_array = category_string.split(';')
    for i in range(len(category_array)):
        category_array[i]  = re.sub('\*', '', category_array[i]).strip()
    return sorted(category_array)

def format_cb_categories(category_string):
    category_array = category_string.split('|')
    return sorted(category_array)

def dedupe_categories(connection):
    '''Execute sql to delete duplicates in categories'''
    print 'de duplicating categories'
    try:
        result = connection.execute(
            '''
            DELETE a
            FROM categories a, categories b
            WHERE (a.name = b.name)
                AND ((a.domain = b.domain)
                        OR (a.domain IS NULL AND b.domain IS NULL))
                AND (a.category = b.category)
                AND (a.id < b.id)
            ''')
    except:
        print traceback.format_exc()
        return

def transform_to_categories(load_pb=False, load_cb=False, engine=None):
    Base.metadata.create_all(engine)

    #Create the session
    session = sessionmaker()
    session.configure(bind=engine)
    s = session()

    if load_cb:
        print 'loading cb data'
        cb_data = s.query(CbCompany).all()
        print 'transforming cb categories'
        for i in range(len(cb_data)):
            categories = format_cb_categories(cb_data[i].category_list)
            for category in categories:
                try:
                    if not category == '':
                        # create a new one if it's not in the database
                        record = build_from_cb(cb_data[i], category)
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
        print 'loading pb data'
        pb_data = s.query(PbRound).all()
        print 'transforming pb categories'
        for i in range(len(pb_data)):
            categories = format_pb_categories(pb_data[i].all_industries)
            for category in categories:
                try:
                    if not category == '':
                        record = build_from_pb(pb_data[i], category)
                        s.add(record) #Add all the records
                except:
                    print 'error in: ' + str(i) + ', ' + cb_data[i].company_name + ':' + traceback.format_exc()
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
        UPDATE categories AS a
        JOIN cb_companies AS c
        ON a.online_profile_url = c.cb_url
        SET a.domain = c.domain
        WHERE a.domain IS NULL
        '''
    )
    connection.close()

    s.close() #Close the connection

if __name__ == "__main__":
    t = time()

    #Create the database
    print 'connecting to mysql database'
    config = ConfigParser.ConfigParser()
    config.read('properties.ini')
    engine = create_engine(config.get('properties', 'engine_string'))

    transform_to_categories(load_cb=True, load_pb=True, engine=engine)

    print "Time elapsed: " + str(time() - t) + " s." #0.091s
