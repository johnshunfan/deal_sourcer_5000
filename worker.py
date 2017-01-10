import os

import cloudstorage as gcs
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import webapp2

from load_sfiq_fc_leads import get_all_list_items
from load_sm_csv import load_from_sm_csv
from load_pb_rounds import load_from_pitchbook, dedupe_pb_rounds
from transform_sm_growth import transform_to_sm_growth

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
        #Create the session
        engine = connect_to_cloudsql_sqlalchemy()
        session = sessionmaker()
        session.configure(bind=engine)

        connection = engine.connect()
        result = connection.execute('DROP TABLE IF EXISTS fc_leads')
        connection.close()

        get_all_list_items(NEWCO_LIST_ID, API_KEY, API_SECRET, engine=engine)
        self.response.write('done')

class LoadPbRoundsHandler(webapp2.RequestHandler):
    def post(self):
        # retrieve file
        filename = self.request.get('filename')
        filename = '/ds5000/' + filename
        new_file = gcs.open(filename)

        # create engine
        engine = connect_to_cloudsql_sqlalchemy()
        session = sessionmaker()
        session.configure(bind=engine)


        load_from_pitchbook(csvfile=new_file, engine=engine)

        # de duplicate
        connection = engine.connect()
        dedupe_pb_rounds(connection)
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
        session = sessionmaker()
        session.configure(bind=engine)

        connection = engine.connect()
        result = connection.execute('DROP TABLE IF EXISTS sm_monthly_revenue')
        connection.close()

        # transform second measure growth
        load_from_sm_csv(csvfile=new_file, engine=engine)

        self.response.write('done')

class TransformSmGrowthHandler(webapp2.RequestHandler):
    def post(self):
        # create engine
        engine = connect_to_cloudsql_sqlalchemy()
        session = sessionmaker()
        session.configure(bind=engine)

        connection = engine.connect()
        result = connection.execute('DROP TABLE IF EXISTS growth')
        connection.close()

        # transform second measure growth
        transform_to_sm_growth(engine)

        self.response.write('done')

app = webapp2.WSGIApplication([
    ('/update_fc_leads', UpdateFcLeadsHandler),
    ('/load_pb_rounds', LoadPbRoundsHandler),
    ('/load_sm_csv', LoadSmCsvHandler),
    ('/transform_sm_growth', TransformSmGrowthHandler),
], debug=True)
