from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import webapp2

from load_sfiq_fc_leads import get_all_list_items

API_KEY = '581312c7e4b04c9692fadf3e'
API_SECRET = '1383QhosG3Eh4JUDoWLTRa0RnFr'
NEWCO_LIST_ID = '56fae761e4b07e602ad2e0fe'

def connect_to_cloudsql_sqlalchemy():
    connection_string = ('mysql+mysqldb://root:password@/test3?unix_socket='
                         '/cloudsql/digital-proton-146222:us-central1:test')
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


app = webapp2.WSGIApplication([
    ('/update_fc_leads', UpdateFcLeadsHandler)
], debug=True)
