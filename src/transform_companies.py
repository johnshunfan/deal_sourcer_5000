#! /usr/bin/python

import csv
import re
import sys
from time import time
import traceback

from sqlalchemy import Column, BigInteger, Integer, Float, DateTime, String, Index
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from load_cb_rounds import CbRound
from load_pb_rounds import PbRound
from sp_util import format_string, format_number, format_date

Base = declarative_base()

class Company(Base):
    __tablename__ = 'companies'
    id = Column(Integer, primary_key=True, nullable=False)
    company_name = Column(String(255))
    primary_industry_group = Column(String(255))
    all_industries = Column(String(255))
    description = Column(String(1023))
    raised_to_date = Column(Float())
    primary_industry_sector = Column(String(255))
    industry_vertical = Column(String(255))
    current_employees = Column(Integer())
    company_website = Column(String(255))
    deal_date = Column(DateTime)
    deal_size = Column(Float())
    post_valuation = Column(Float())
    investors = Column(String(1023))
    series = Column(String(255))
    deal_type = Column(String(255))
    online_profile_url = Column(String(255))
    revenue_growth_1mo = Column(Float())
    last_month_revenue = Column(Float())
    fc_lead = Column(String(255))
    comment_url = Column(String(1023))
    __table_args__ = (
            Index('name', 'company_name'),
            Index('domain', 'company_website'),
            Index('deal', 'deal_date', 'deal_size'),
            Index('last', 'last_month_revenue'),
            Index('fcl', 'fc_lead'))

def build_from_cb(data, connection):
    # get all categories in a neatly formatted list
    result = connection.execute(
            '''
            SELECT GROUP_CONCAT(
                DISTINCT c.category
                ORDER BY c.category
                ASC SEPARATOR ', ') AS categories
            FROM categories c
            WHERE c.domain = '{0}'
            '''.format(data.domain)
    )
    categories = None
    if not result is None:
        categories = result.fetchone().categories

    # get all investors in a neatly formatted list
    result = connection.execute(
            '''
            SELECT GROUP_CONCAT(
                DISTINCT i.investor
                ORDER BY i.investor
                ASC SEPARATOR ', ') AS investors
            FROM investors i
            WHERE i.domain = '{0}'
            '''.format(data.domain)
    )
    investors = None
    if not result is None:
        investors = result.fetchone().investors

    # build object
    return Company(**{
        'company_name': data.company_name,
        'all_industries': categories,
        'description': data.short_description,
        'raised_to_date': data.funding_total_usd,
        'company_website': data.domain,
        'deal_date': data.announced_on,
        'deal_size': data.raised_amount_usd,
        'post_valuation': data.post_money_valuation_usd,
        'investors': investors,
        'deal_type': data.funding_round_type,
        'online_profile_url': data.cb_url,
        'revenue_growth_1mo':data.one_month,
        'last_month_revenue':data.last_revenue
    })

def build_from_pb(data, connection):
    # get all categories in a neatly formatted list
    result = connection.execute(
            '''
            SELECT GROUP_CONCAT(
                DISTINCT c.category
                ORDER BY c.category
                ASC SEPARATOR ', ') AS categories
            FROM categories c
            WHERE c.domain = '{0}'
            '''.format(data.company_website)
    )
    categories = None
    if not result is None:
        categories = result.fetchone().categories

    # get all investors in a neatly formatted list
    result = connection.execute(
            '''
            SELECT GROUP_CONCAT(
                DISTINCT i.investor
                ORDER BY i.investor
                ASC SEPARATOR ', ') AS investors
            FROM investors i
            WHERE i.domain = '{0}'
            '''.format(data.company_website)
    )
    investors = None
    if not result is None:
        investors = result.fetchone().investors

    # TODO make field null if they are ''
    # build object
    return Company(**{
        'company_name': data.company_name,
        'primary_industry_group': data.primary_industry_group,
        'all_industries': categories,
        'description': data.description,
        'raised_to_date': data.raised_to_date,
        'primary_industry_sector': data.primary_industry_sector,
        'industry_vertical': data.industry_vertical,
        'current_employees': data.current_employees,
        'company_website': data.company_website,
        'deal_date': data.deal_date,
        'deal_size': data.deal_size,
        'post_valuation': data.post_valuation,
        'investors': investors,
        'series': data.series,
        'deal_type': data.deal_type,
        'online_profile_url': data.pitchbook_link,
        'revenue_growth_1mo':data.one_month,
        'last_month_revenue':data.last_revenue
        })

def dedupe_companies(connection):
    print 'deduplicating companies'
    try:
        result = connection.execute(
            '''
            SELECT
                c.*
            FROM
                companies c
            JOIN (
                SELECT
                    company_name,
                    company_website,
                    count(*)
                FROM companies
                GROUP BY
                    company_name,
                    company_website
                HAVING count(*) > 1
                ) c2
             ON c.company_name = c2.company_name
                AND (c.company_website = c2.company_website
                     OR c2.company_website IS NULL)
            ORDER BY c.id
            ''')
    except:
        print traceback.format_exc()
        return
    company_dict = {}
    print 'query returned'
    for company in result:
        print str(company)
        cw = company.company_website
        if cw is None:
            cw = company.company_name
        cw = cw.lower()
        if cw in company_dict:
            # delete one if it's a double
            print 'already found ' + cw
            if company.online_profile_url.find('pitchbook') != -1:
                print company.online_profile_url + ' contains pitchbook'
                # if both current row and stored row are pitchbook
                if (company_dict[cw].online_profile_url.find('pitchbook') != -1) \
                        and (company_dict[cw].deal_date > company.deal_date):
                    # if stored company has pitchbook, and is a later entry
                    print 'deleting new ' + company.online_profile_url
                    delete_company(company, connection)
                else:
                    print 'deleting existing ' + company_dict[cw].online_profile_url
                    delete_company(company_dict[cw], connection)
                    company_dict[cw] = company
            else:
                print company.online_profile_url + ' does not contain pitchbook'
                if (company_dict[cw].online_profile_url.find('pitchbook') == -1) \
                        and (company_dict[cw].deal_date > company.deal_date):
                    print 'deleting new ' + company.online_profile_url + ' old deal date: ' + str(company_dict[cw].deal_date) \
                        + ' new deal date: ' + str(company.deal_date)
                    delete_company(company, connection)
                else:
                    print 'deleting existing ' + company_dict[cw].online_profile_url
                    delete_company(company_dict[cw], connection)
                    company_dict[cw] = company
        else:
            company_dict[cw] = company

def delete_company(company, connection):
    result = connection.execute(
        '''
        DELETE FROM companies
        WHERE id={0}
        '''.format(company.id))

def create_interest_url(connection):
    result = connection.execute(
        '''
UPDATE companies x
JOIN companies y
    ON x.id = y.id
SET x.interest_url=CONCAT('
    <html>
        <body>
            <script type="text/javascript">
                function doPreview(rowId) {
                    form=document.getElementById("interest"+rowId);
                    form.target="_blank";
                    form.action="https://digital-proton-146222.appspot.com/interest"; form.submit();
                }
            </script>
            <form id="interest', y.id, '" method="post">
                <input type="hidden" name="row_id" value="', y.id,'"/>
                <select name="interest">
                    <option value=""></option>
                    <option value="AS">AS</option>
                    <option value="AG">AG</option>
                    <option value="CM">CM</option>
                    <option value="JC">JC</option>
                    <option value="JE">JE</option>
                    <option value="JM">JM</option>
                    <option value="MS">MS</option>
                    <option value="PH">PH</option>
                    <option value="RG">RG</option>
                    <option value="SV">SV</option>
                    <option value="ZN">ZN</option>
                </select>
                <button onclick="doPreview(', y.id, ');">Submit</button>
            </form>
        </body>
    </html>')
        ''')

def create_comment_url(connection):
    result = connection.execute(
        '''
UPDATE companies x
JOIN companies y
    ON x.id = y.id
SET x.comment_url=CONCAT('
    <html>
        <body>
            <script type="text/javascript">
                function doPreview(rowId) {
                    form=document.getElementById("form"+rowId);
                    form.target="_blank";
                    form.action="https://digital-proton-146222.appspot.com/comment"; form.submit();
                }
            </script>
            <form id="form', y.id, '" method="post">
                <input type="hidden" name="row_id" value="', y.id,'"/>
                <input type="text" name="comment" value="new comment"/>
                <button onclick="doPreview(', y.id, ');">Submit</button>
            </form>
        </body>
    </html>')
        ''')

def transform_to_companies(load_cb=False, load_pb=False, engine=None):
    Base.metadata.create_all(engine)
    session = sessionmaker()
    session.configure(bind=engine)
    s = session()

    # Load from Crunchbase
    connection = engine.connect()
    if load_cb:
        print 'loading cb data'
        # this needs to the be max based on deal date
        result = connection.execute(
            '''
            SELECT DISTINCT
                            c.*,
                            cbr.funding_round_type,
                            cbr.funding_round_code,
                            cbr.announced_on,
                            cbr.raised_amount_usd,
                            cbr.post_money_valuation_usd,
                            cbr.investor_names,
                            g.one_month,
                            g.last_revenue
            FROM cb_companies c
            JOIN (
                SELECT cbr2.*
                FROM cb_rounds cbr2
                JOIN (
                    SELECT
                        MAX(cbr4.id) AS id
                    FROM cb_rounds cbr4
                    JOIN (
                        SELECT
                               cbr6.cb_url,
                               MAX(cbr6.announced_on) AS announced_on
                        FROM cb_rounds cbr6
                        GROUP BY cbr6.company_name, cbr6.cb_url
                    ) cbr5
                    ON cbr4.cb_url = cbr5.cb_url
                        AND cbr4.announced_on = cbr5.announced_on
                    GROUP BY cbr4.company_name, cbr4.cb_url
                ) cbr3
                ON cbr2.id = cbr3.id
            ) cbr
            ON c.cb_url = cbr.cb_url
            LEFT JOIN growth g
            ON c.domain = g.domain
            '''
        )

        cb_data = []
        for c in result:
            cb_data.append(c)
        print 'loading cb to database'
        for i in range(len(cb_data)):
            try:
                connection.close()
                connection = engine.connect()
                record = build_from_cb(cb_data[i], connection)
                s.add(record)
            except:
                print 'error in: ' \
                    + str(i) + ', ' \
                    + cb_data[i][1] \
                    + ':' + traceback.format_exc()
            if (i % 1000 == 0 or i == len(cb_data) - 1):
                print 'index: ' + str(i)
                # if can't commit, then rollback
                try:
                    s.commit()
                except:
                    s.rollback() #Rollback the changes on error
                    print 'Unexpected error on index ' \
                        + str(i) + ':' + traceback.format_exc()
                    break
    connection.close()

    # Load from PitchBook
    connection = engine.connect()
    if load_pb:
        print 'transform comapnies: loading pb data'
        # this needs to the be max based on deal date
        result = connection.execute(
            '''
            SELECT p.*,
                   g.one_month,
                   g.last_revenue
            FROM pb_rounds p
            JOIN (
                SELECT pbr.pitchbook_link,
                       MAX(pbr.deal_date) as date,
                       MAX(pbr.deal_size) as size
                FROM pb_rounds pbr
                GROUP BY pitchbook_link
            ) pb
            ON p.pitchbook_link = pb.pitchbook_link
                AND p.deal_size = pb.size
            LEFT JOIN growth g
            ON p.company_website = g.domain
            '''
        )

        pb_data = []
        for c in result:
            pb_data.append(c)

        print 'transform companies: loading pb to database'
        for i in range(len(pb_data)):
            print pb_data[i].company_name, \
                pb_data[i].deal_date, \
                pb_data[i].deal_size

            try:
                connection.close()
                connection = engine.connect()
                result = connection.execute(
                        '''
                        SELECT *
                        FROM companies
                        WHERE company_name="{0}"
                            AND company_website='{1}'
                        '''.format(pb_data[i].company_name,
                                   pb_data[i].company_website))
                if result.rowcount == 0:
                    record = build_from_pb(pb_data[i], connection)
                    s.add(record)
            except:
                print 'error in: ' \
                    + str(i) + ', ' + pb_data[i][1] + ':' \
                    + traceback.format_exc()
            if (i % 1000 == 0 or i == len(pb_data) - 1):
                print 'index: ' + str(i)
                # if can't commit, then rollback
                try:
                    s.commit()
                except:
                    s.rollback() #Rollback the changes on error
                    print 'Unexpected error on index ' + str(i) + ':' \
                        + traceback.format_exc()
                    break
    connection.close()

    print 'adding comment url'
    connection.close()
    connection = engine.connect()
    create_comment_url(connection)

    s.close() #Close the connection

if __name__ == "__main__":
    t = time()

    #Create the database
    print 'connecting to mysql database'
    config = ConfigParser.ConfigParser()
    config.read('properties.ini')
    engine = create_engine(config.get('properties', 'engine_string'))

    transform_to_companies(load_cb=True, load_pb=True, engine=engine)

    print "Time elapsed: " + str(time() - t) + " s." #0.091s
