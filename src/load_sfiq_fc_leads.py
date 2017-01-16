#! /usr/bin/python

# load all of salesforce IQ fc leads into the database
import csv
import re
import requests
import requests_toolbelt.adapters.appengine
import traceback
import json
from time import time
from sqlalchemy import Column, BigInteger, Integer, Float, DateTime, String, Index
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sp_util import format_string, format_date

# Use the App Engine Requests adapter. This makes sure that Requests uses
# URLFetch.
requests_toolbelt.adapters.appengine.monkeypatch()

def printJ(jData):
    print json.dumps(jData, indent=4, sort_keys=True)

def get_user_name(userId, API_KEY, API_SECRET):
    getUserUrl = 'https://api.salesforceiq.com/v2/users/{userId}'
    getUserUrl = getUserUrl.replace('{userId}', userId)
    response = requests.get(getUserUrl, auth=(API_KEY, API_SECRET))
    data = json.loads(response.text)
    printJ(data)
    return json.loads(response.text)['name']

def select_list_items(list_id, start, limit, API_KEY, API_SECRET):
    insertUrl = 'https://api.salesforceiq.com/v2/lists/{0}/listitems?_start={1}&_limit={2}'.format(list_id, start, limit)
    response = requests.get(insertUrl, auth=(API_KEY, API_SECRET))
    data = json.loads(response.text)
    #printJ(data)
    return data

Base = declarative_base()

class FcLead(Base):
    __tablename__ = 'fc_leads'
    #tell SQLAlchemy the name of column and its attributes:
    id = Column(Integer, primary_key=True, nullable=False)
    name = Column(String(255))
    domain = Column(String(255))
    fc_lead = Column(String(255))
    __table_args__ = (Index('name', 'name'), Index('fcl', 'fc_lead'))

def get_fc_leads(fc_lead_dict, companies, session, API_KEY, API_SECRET):
    s = session
    print 'looking up and loading'
    #companies = get_all_list_items(NEWCO_LIST_ID, API_KEY, API_SECRET)
    #companies = select_list_items(NEWCO_LIST_ID, 2000, 200, API_KEY, API_SECRET)
    for i in range(len(companies['objects'])):
        fc_lead_id = companies['objects'][i]['fieldValues']['1'][0]['raw']
        fc_lead_str = ''
        if not fc_lead_id in fc_lead_dict:
            print 'looking up ' + str(fc_lead_id)
            try:
                fc_lead_dict[fc_lead_id] = get_user_name(fc_lead_id, API_KEY, API_SECRET)
            except:
                fc_lead_dict[fc_lead_id] = fc_lead_id
            print 'looking up ' + str(fc_lead_id) + ': ' + fc_lead_dict[fc_lead_id]
        fc_lead_str = fc_lead_dict[fc_lead_id]
        try:
            domain = companies['objects'][i]['fieldValues']['87'][0]['raw']
        except:
            domain = ''
        try:
            record = FcLead(**{
                'name':companies['objects'][i]['name'],
                'domain':domain,
                'fc_lead':fc_lead_str,
            })
            s.add(record) #Add all the records
        except:
            print 'error in: ' + str(i) + ', ' + companies['objects'][i]['name'] + ':' + traceback.format_exc()
        if (i % 1000 == 0 or i == len(companies['objects']) - 1):
            print 'index: ' + str(i)
            # if can't commit, then rollback
            try:
                s.commit()
            except:
                s.rollback() #Rollback the changes on error
                print 'Unexpected error on index ' + str(i) + ':' + traceback.format_exc()
                break

    print 'loaded data'
    return fc_lead_dict

def combine_with_fc_lead(connection):
    print 'combining companies and fc leads'
    result = connection.execute(
        '''
        UPDATE companies c
        INNER JOIN fc_leads fc
        ON c.company_website = fc.domain
            OR c.company_name = fc.name
        SET c.fc_lead = fc.fc_lead
        ''')

def get_all_list_items(list_id, API_KEY, API_SECRET, limit = 0, engine = None):
    Base.metadata.create_all(engine)

    #Create the session
    session = sessionmaker()
    session.configure(bind=engine)
    s = session()

    size = select_list_items(list_id, 0, 0, API_KEY, API_SECRET)['totalSize'] if limit == 0 else limit
    print 'size=' + str(size)
    index = 1
    fc_lead_dict = {}
    while index <= size:
        print 'begin loading index: ' + str(index) + ' to: ' + str(index+200)
        data = select_list_items(list_id, index, 200, API_KEY, API_SECRET)
        fc_lead_dict = get_fc_leads(fc_lead_dict,
                                    data, session=s,
                                    API_KEY=API_KEY,
                                    API_SECRET=API_SECRET)
        print 'finished loading index: ' + str(index) + ' to: ' + str(index+200)
        index += 200
    print 'completed all iterations of loading data'

    #userId = (item for item in data['objects'] if item['name'].lower() == companyName.lower()).next()['fieldValues']['1'][0]['raw']

if __name__ == "__main__":
    API_KEY= '581312c7e4b04c9692fadf3e'
    API_SECRET= '1383QhosG3Eh4JUDoWLTRa0RnFr'
    NEWCO_LIST_ID = '56fae761e4b07e602ad2e0fe'

    t = time()

    #Create the database
    print 'connecting to mysql database'
    engine = create_engine('mysql://root@127.0.0.1/test3?charset=utf8mb4')
    Base.metadata.create_all(engine)

    print get_user_name('570s19794e4b08cb4a836fd52', API_KEY, API_SECRET)
    #get_all_list_items(NEWCO_LIST_ID, API_KEY, API_SECRET, limit=5, engine=engine)
