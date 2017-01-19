#! /usr/bin/python
import csv
import re
from time import time
import traceback

from sqlalchemy import BigInteger, Column, create_engine, DateTime, Float, \
    Index, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from sp_util import format_aa_date,format_number, format_string

Base = declarative_base()

class AaGrowth(Base):
    '''SQLAlchemy model for App Annie app'''
    __tablename__ = 'aa_growth'

    id = Column(Integer, primary_key=True, nullable=False)
    name = Column(String(255))
    domain = Column(String(255))
    mau = Column(BigInteger)
    mau1 = Column(BigInteger)
    mau3 = Column(BigInteger)
    period = Column(DateTime)
    mau_growth1 = Column(Float)
    mau_growth3 = Column(Float)

    __table_args__ = (
        Index('mau', 'mau'),
        Index('name', 'name'),
        Index('domain', 'domain'))

class AaMonth(Base):
    '''SQLAlchemy model for App Annie app'''
    __tablename__ = 'aa_months'

    id = Column(Integer, primary_key=True, nullable=False)
    period = Column(DateTime)
    app_id = Column(String(255))
    app_name = Column(String(255))
    app_category = Column(String(255))
    publisher_id = Column(String(255))
    publisher_name = Column(String(255))
    company_name = Column(String(255))
    parent_company_name = Column(String(255))
    unified_app_name = Column(String(255))
    app_franchise = Column(String(255))
    unified_app_id = Column(String(255))
    usage_penetration = Column(String(255))
    install_penetration = Column(String(255))
    open_rate = Column(String(255))
    active_users = Column(BigInteger)
    total_minutes = Column(String(255))
    avg_sess_per_usr = Column(String(255))
    avg_sess_dur = Column(String(255))
    avg_time_per_usr = Column(String(255))

    __table_args__ = (
        Index('my_app_id', 'period', 'app_id'),
        Index('name', 'company_name'),
        Index('active_users', 'active_users'))

def dedupe_aa_months(connection):
    '''Execute sql to delete duplicates in aa_months'''
    print 'de duplicating aa_months'
    try:
        connection.execute(
            '''
            DELETE a
            FROM aa_months a, aa_months b
            WHERE (a.period = b.period)
                AND (a.unified_app_id = b.unified_app_id)
                AND (a.id > b.id)
            ''')
    except:
        print traceback.format_exc()
        return

def get_app_by_month(engine, app_id, offset):
    """ Get the info for the app with the specified offset months."""
    connection = engine.connect()
    result = connection.execute(
        '''
        SELECT a.company_name,
            a.active_users,
            a.publisher_id,
            a.app_id
        FROM aa_months a
        WHERE a.app_id='{0}'
            AND a.period >= DATE_SUB((SELECT MAX(period) FROM aa_months), INTERVAL {1} MONTH)
            AND a.period < DATE_ADD(
                DATE_SUB(
                    (SELECT MAX(period) FROM aa_months), INTERVAL {1} MONTH
                ),
                INTERVAL 1 DAY
            )
        LIMIT 1
        '''.format(app_id, offset))
    connection.close()
    return result.fetchone()

def get_most_popular_app(engine, publisher_id):
    """ Get the most popular app of a company for the most recent period."""
    connection = engine.connect()
    result = connection.execute(
        '''
        SELECT a.period,
            a.company_name,
            a.active_users,
            a.publisher_id,
            a.app_id
        FROM aa_months a
        WHERE a.publisher_id='{0}'
            AND a.period >= (SELECT MAX(period) FROM aa_months)
            AND a.period < DATE_ADD((SELECT MAX(period) FROM aa_months), INTERVAL 1 DAY)
        ORDER BY a.active_users DESC
        LIMIT 1
        '''.format(publisher_id))
    connection.close()
    return result.fetchone()

def load_from_aa(csvfile, engine):
    Base.metadata.create_all(engine)

    #Create the session
    session = sessionmaker()
    session.configure(bind=engine)
    s = session()

    print 'loading file'
    rounds = [row for row in csv.reader(csvfile.read().splitlines())]
    # edit for different types of CSV
    data = list(rounds)[6:-4]
    print 'loaded file'

    print 'loading to database'
    print data[0]
    print data[len(data)-1]
    for i in range(0, len(data)):
    #for i in range(0, 5):
        try:
            record = AaMonth(**{
                'period':format_aa_date(data[i][3]),
                'app_id':data[i][4],
                'app_name':format_string(data[i][5]).encode('utf-8'),
                'app_category':data[i][6],
                'publisher_id':data[i][7],
                'publisher_name':data[i][8],
                'company_name':data[i][9],
                'parent_company_name':data[i][10],
                'unified_app_name':data[i][11],
                'app_franchise':data[i][12],
                'unified_app_id':data[i][13],
                'usage_penetration':data[i][17],
                'install_penetration':data[i][18],
                'open_rate':data[i][19],
                'active_users':format_number(data[i][20]),
                'total_minutes':data[i][21],
                'avg_sess_per_usr':data[i][22],
                'avg_sess_dur':data[i][23],
                'avg_time_per_usr':data[i][24]
            })
            s.add(record) #Add all the records
        except:
            print 'error in: ' + str(i) + ', ' + data[i][5] + ':' + traceback.format_exc()
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

def transform_aa_growth(engine):
    """ Calculate App Annie growth from database."""
    Base.metadata.create_all(engine)

    #Create the session
    session = sessionmaker()
    session.configure(bind=engine)
    s = session()

    for pid in s.query(AaMonth.publisher_id).distinct():
        try:
            # apple = 284417353
            app = get_most_popular_app(engine=engine,
                                       publisher_id=pid.publisher_id)
            app1 = get_app_by_month(engine=engine, app_id=app.app_id, offset=1)
            app3 = get_app_by_month(engine=engine, app_id=app.app_id, offset=3)

            mau = app.active_users
            mau1 = None
            mau3 = None
            mau_growth1 = None
            mau_growth3 = None
            if mau is not None:
                if app1 is not None and app1.active_users is not None:
                    mau1 = app1.active_users;
                    mau_growth1 = \
                        format((mau - mau1) / float(mau1) * 100, '.0f')
                if app3 is not None and app3.active_users is not None:
                    mau3 = app3.active_users;
                    mau_growth3 = \
                        format((mau - mau3) / float(mau3) * 100, '.0f')

            record = AaGrowth(**{
                'name':app.company_name,
                'mau':mau,
                'mau1':mau1,
                'mau3':mau3,
                'period':app.period,
                'mau_growth1':mau_growth1,
                'mau_growth3':mau_growth3
            })

            s.add(record) #Add all the records
        except:
            print pid.publisher_id+ ': ' + traceback.format_exc()
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

    connection = engine.connect()
    result = connection.execute('DROP TABLE IF EXISTS aa_months')
    connection.close()

    with open('../../1old_ds5000/data/App_Annie_Usage_Intelligence_Top_Usage_iPhone_United States_Applications_2016-09-01_2016-12-31.csv', 'rU') as csvfile:
        load_from_aa(csvfile=csvfile, engine=engine)

    connection = engine.connect()
    dedupe_aa_months(connection)
    connection.close()

    connection = engine.connect()
    result = connection.execute('DROP TABLE IF EXISTS aa_growth')
    connection.close()

    Base.metadata.create_all(engine)

    transform_aa_growth(engine)

    print "Time elapsed: " + str(time() - t) + " s." #0.091s
