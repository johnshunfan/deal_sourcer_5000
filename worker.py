from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import webapp2

from load_sfiq_fc_leads import get_all_list_items

CLOUDSQL_CONNECTION_NAME = os.environ.get('CLOUDSQL_CONNECTION_NAME')
CLOUDSQL_USER = os.environ.get('CLOUDSQL_USER')
CLOUDSQL_PASSWORD = os.environ.get('CLOUDSQL_PASSWORD')
API_KEY = os.environ.get('API_KEY')
API_SECRET = os.environ.get('API_SECRET')
NEWCO_LIST_ID = os.environ.get('NEWCO_LIST_ID')

def connect_to_cloudsql_sqlalchemy():
    connection_string =
        ('mysql+mysqldb://{0}:{1}@/test3?unix_socket=/cloudsql/{2}')
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


app = webapp2.WSGIApplication([
    ('/update_fc_leads', UpdateFcLeadsHandler)
], debug=True)
