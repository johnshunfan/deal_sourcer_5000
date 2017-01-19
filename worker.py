import os

import cloudstorage as gcs
from sqlalchemy import create_engine
import webapp2

from src.load_app_annie import load_from_aa, dedupe_aa_months, \
    transform_aa_growth
from src.load_pb_rounds import load_from_pitchbook, dedupe_pb_rounds
from src.load_sfiq_fc_leads import get_all_list_items, combine_with_fc_lead
from src.load_sm_csv import load_from_sm_csv
from src.transform_categories import transform_to_categories, dedupe_categories
from src.transform_companies import transform_to_companies, dedupe_companies
from src.transform_investors import transform_to_investors, dedupe_investors
from src.transform_sm_growth import transform_to_sm_growth, combine_with_growth

CLOUDSQL_CONNECTION_NAME = os.environ.get('CLOUDSQL_CONNECTION_NAME')
CLOUDSQL_USER = os.environ.get('CLOUDSQL_USER')
CLOUDSQL_PASSWORD = os.environ.get('CLOUDSQL_PASSWORD')
API_KEY = os.environ.get('API_KEY')
API_SECRET = os.environ.get('API_SECRET')
NEWCO_LIST_ID = os.environ.get('NEWCO_LIST_ID')

def connect_to_cloudsql_sqlalchemy():
    connection_string = \
        ('mysql+mysqldb://{0}:{1}@/test3?unix_socket=/cloudsql/{2}') \
            .format(CLOUDSQL_USER, CLOUDSQL_PASSWORD, CLOUDSQL_CONNECTION_NAME)
    engine = create_engine(connection_string)
    return engine

class UpdateFcLeadsHandler(webapp2.RequestHandler):
    def post(self):
        #Create the engine
        engine = connect_to_cloudsql_sqlalchemy()

        connection = engine.connect()
        result = connection.execute('DROP TABLE IF EXISTS fc_leads')
        connection.close()

        get_all_list_items(NEWCO_LIST_ID, API_KEY, API_SECRET, engine=engine)

        # combine companies with FC Leads
        connection = engine.connect()
        combine_with_fc_lead(connection)
        connection.close()

        self.response.write('done')

class LoadAppAnnieHandler(webapp2.RequestHandler):
    def post(self):
        # retrieve file
        filename = self.request.get('filename')
        filename = '/ds5000/' + filename
        new_file = gcs.open(filename)

        # create engine
        engine = connect_to_cloudsql_sqlalchemy()

        # load app annie months
        load_from_aa(csvfile=new_file, engine=engine)

        # remove duplicates
        connection = engine.connect()
        dedupe_aa_months(connection)
        connection.close()

        # remove old growth table
        connection = engine.connect()
        result = connection.execute('DROP TABLE IF EXISTS aa_growth')
        connection.close()

        # transform app annie growth
        transform_aa_growth(engine)

class LoadPbRoundsHandler(webapp2.RequestHandler):
    def post(self):
        # retrieve file
        filename = self.request.get('filename')
        filename = '/ds5000/' + filename
        new_file = gcs.open(filename)

        # create engine
        engine = connect_to_cloudsql_sqlalchemy()

        # load pitchbook rounds
        load_from_pitchbook(csvfile=new_file, engine=engine)

        # de-duplicate pitchbook rounds
        connection = engine.connect()
        dedupe_pb_rounds(connection)
        connection.close()

        # transform categories
        transform_to_categories(load_pb=True, load_cb=False, engine=engine)

        # de-duplicate categories
        connection = engine.connect()
        dedupe_categories(connection)
        connection.close()

        # transform investors
        transform_to_investors(load_pb=True, load_cb=False, engine=engine)

        # de-duplicate investors
        connection = engine.connect()
        dedupe_investors(connection)
        connection.close()

        # rebuild companies
        transform_to_companies(load_pb=True, load_cb=False, engine=engine)

        # de-duplicate companies
        connection = engine.connect()
        dedupe_companies(connection)
        connection.close()

        # combine companies with FC Leads
        connection = engine.connect()
        combine_with_fc_lead(connection)
        connection.close()

        self.response.write('done')

class LoadSmCsvHandler(webapp2.RequestHandler):
    def post(self):
        # retrieve file
        filename = self.request.get('filename')
        filename = '/ds5000/' + filename
        new_file = gcs.open(filename)

        # create engine
        engine = connect_to_cloudsql_sqlalchemy()

        connection = engine.connect()
        result = connection.execute('DROP TABLE IF EXISTS sm_monthly_revenue')
        connection.close()

        # load second measure csv
        load_from_sm_csv(csvfile=new_file, engine=engine)

        connection = engine.connect()
        result = connection.execute('DROP TABLE IF EXISTS growth')
        connection.close()

        # transform second measure growth
        transform_to_sm_growth(engine)

        # combine companies with growth
        connection = engine.connect()
        combine_with_growth(connection)
        connection.close()

        self.response.write('done')


class TransformSmGrowthHandler(webapp2.RequestHandler):
    def post(self):
        # create engine
        engine = connect_to_cloudsql_sqlalchemy()

        connection = engine.connect()
        result = connection.execute('DROP TABLE IF EXISTS growth')
        connection.close()

        # transform second measure growth
        transform_to_sm_growth(engine)

        self.response.write('done')

app = webapp2.WSGIApplication([
    ('/load_app_annie', LoadAppAnnieHandler),
    ('/load_pb_rounds', LoadPbRoundsHandler),
    ('/load_sm_csv', LoadSmCsvHandler),
    ('/transform_sm_growth', TransformSmGrowthHandler),
    ('/update_fc_leads', UpdateFcLeadsHandler),
], debug=True)
