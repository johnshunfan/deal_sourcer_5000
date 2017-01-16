#! /usr/bin/python

# take the pb csv of companies (excel export) and turn into a database
# this template
# http://stackoverflow.com/questions/31394998/using-sqlalchemy-to-load-csv-file-into-a-database
# how to make a string column
# http://docs.sqlalchemy.org/en/latest/core/metadata.html#creating-and-dropping-database-tables
import csv
import ConfigParser
import traceback
import re
from time import time
from sqlalchemy import Column, BigInteger, Integer, Float, DateTime, String, Index
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sp_util import format_string, format_number, format_date, format_float, format_domain

Base = declarative_base()

class PbRound(Base):
    '''SQLAlchemy model for a Pitchbook Round'''
    __tablename__ = 'pb_rounds'
    #tell SQLAlchemy the name of column and its attributes:
    id = Column(Integer, primary_key=True, nullable=False)
    company_name = Column(String(255))
    primary_industry_group = Column(String(255))
    all_industries = Column(String(255))
    vc_round = Column(String(255))
    company_id = Column(String(255))
    description = Column(String(1023))
    new_investors = Column(String(1023))
    lead_investors = Column(String(1023))
    company_state_province = Column(String(255))
    raised_to_date = Column(Float())
    primary_industry_sector = Column(String(255))
    industry_vertical = Column(String(255))
    current_employees = Column(Integer())
    company_website = Column(String(255))
    deal_date = Column(DateTime)
    deal_size = Column(Float())
    pre_money_valuation = Column(Float())
    post_valuation = Column(Float())
    investors = Column(String(1023))
    series = Column(String(255))
    deal_type = Column(String(255))
    pitchbook_link = Column(String(255))
    __table_args__ = (
        Index('company_id', 'company_name'),
        Index('company_website', 'company_website'))

def delete_old_table(table_name, connection):
    connection.execute(
        '''
        drop table {0}
        '''.format(table_name)
    )

def dedupe_pb_rounds(connection):
    '''Execute sql to delete duplicates in pb_rounds'''
    print 'de duplicating pb_rounds'
    try:
        connection.execute(
            '''
            DELETE a
            FROM pb_rounds a, pb_rounds b
            WHERE (a.pitchbook_link = b.pitchbook_link)
                AND (a.deal_date < b.deal_date)
            ''')
        connection.execute(
            '''
            DELETE a
            FROM pb_rounds a, pb_rounds b
            WHERE (a.pitchbook_link = b.pitchbook_link)
                AND (a.deal_date = b.deal_date)
                AND (a.id < b.id)
            ''')
    except:
        print traceback.format_exc()
        return

def load_from_pitchbook(csvfile, engine):
    Base.metadata.create_all(engine)

    #Create the session
    session = sessionmaker()
    session.configure(bind=engine)
    s = session()

    print 'loading file'
    rounds = [row for row in csv.reader(csvfile.read().splitlines())]
    # edit for different types of CSV
    data = list(rounds)[8:-3]
    print 'loaded file'

    print 'loading to database'
    for i in range(0, len(data)):
    #for i in range(0, 5):
        try:
            record = PbRound(**{
                'company_name':data[i][1],
                'primary_industry_group':data[i][2],
                'all_industries':data[i][3],
                'vc_round':data[i][4],
                'company_id':data[i][5],
                'description':data[i][6],
                'new_investors':data[i][7],
                'lead_investors':data[i][8],
                'company_state_province':data[i][9],
                'raised_to_date':format_float(data[i][10]),
                'primary_industry_sector':data[i][11],
                'industry_vertical':data[i][12],
                'current_employees':format_number(data[i][13]),
                'company_website':format_domain(data[i][14]),
                'deal_date':format_date(data[i][15], '%d-%b-%Y'),
                'deal_size':format_float(data[i][16]),
                'pre_money_valuation':format_float(data[i][17]),
                'post_valuation':format_float(data[i][18]),
                'investors':data[i][19],
                'series':data[i][20],
                'deal_type':data[i][21],
                'pitchbook_link':'https://my.pitchbook.com?c=' + data[i][0]
            })
            # check if the record is complete
            if not ((data[i][1] == '') and (data[i][14] == '')):
                s.add(record) #Add all the records
        except:
            print 'error in: ' + str(i) + ', ' + data[i][1] + ':' + traceback.format_exc()
        if i % 1000 == 0 or i == len(data) - 1:
            print 'index: ' + str(i)
    print 'committing'
    try:
        s.commit()
    except:
        s.rollback()
        print traceback.format_exc()
    finally:
        s.close() #Close the connection

if __name__ == "__main__":
    t = time()

    #Create the database
    print 'connecting to mysql database'
    engine = create_engine('mysql://root@127.0.0.1/test3?charset=utf8mb4')
    Base.metadata.create_all(engine)

    connection = engine.connect()
    result = connection.execute('DROP TABLE IF EXISTS pb_rounds')
    connection.close()

    with open('../../1old_ds5000/data/pitchbook/pitchbook_20161001_20161015.csv', 'rU') as csvfile:
        load_from_pitchbook(csvfile=csvfile, engine=engine)

    with open('../../1old_ds5000/data/pitchbook/pitchbook_20161016_20161031.csv', 'rU') as csvfile:
        load_from_pitchbook(csvfile=csvfile, engine=engine)

    with open('../../1old_ds5000/data/pitchbook/pitchbook_20161101_20161115.csv', 'rU') as csvfile:
        load_from_pitchbook(csvfile=csvfile, engine=engine)

    with open('../../1old_ds5000/data/pitchbook/pitchbook_20161116_20161130.csv', 'rU') as csvfile:
        load_from_pitchbook(csvfile=csvfile, engine=engine)

    #with open('../../1old_ds5000/data/pitchbook/pitchbook_20161201_20161215.csv', 'rU') as csvfile:
    #    load_from_pitchbook(csvfile=csvfile, engine=engine)

    #with open('../../1old_ds5000/data/pitchbook/pitchbook_20161216_20161231.csv', 'rU') as csvfile:
    #    load_from_pitchbook(csvfile=csvfile, engine=engine)

    connection = engine.connect()
    dedupe_pb_rounds(connection)
    connection.close()
    print "Time elapsed: " + str(time() - t) + " s." #0.091s
