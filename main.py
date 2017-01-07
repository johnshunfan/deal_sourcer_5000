from flask import Flask, request
from time import time
import os
import MySQLdb
from sqlalchemy import Column, BigInteger, Integer, Float, DateTime, String, Index
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import webapp2

from load_sfiq_fc_leads import get_all_list_items

# Note: We don't need to call run() since our application is embedded within
# the App Engine WSGI application server.

# These environment variables are configured in app.yaml.
CLOUDSQL_CONNECTION_NAME = os.environ.get('CLOUDSQL_CONNECTION_NAME')
CLOUDSQL_USER = os.environ.get('CLOUDSQL_USER')
CLOUDSQL_PASSWORD = os.environ.get('CLOUDSQL_PASSWORD')
# variables for refresh newco
API_KEY= '581312c7e4b04c9692fadf3e'
API_SECRET= '1383QhosG3Eh4JUDoWLTRa0RnFr'
NEWCO_LIST_ID = '56fae761e4b07e602ad2e0fe'

# initialize flask
app = Flask(__name__)
app.config['DEBUG'] = True

def connect_to_cloudsql():
    # When deployed to App Engine, the `SERVER_SOFTWARE` environment variable
    # will be set to 'Google App Engine/version'.
    if os.getenv('SERVER_SOFTWARE', '').startswith('Google App Engine/'):
        # Connect using the unix socket located at
        # /cloudsql/cloudsql-connection-name.
        cloudsql_unix_socket = os.path.join(
            '/cloudsql', CLOUDSQL_CONNECTION_NAME)

        db = MySQLdb.connect(
            unix_socket=cloudsql_unix_socket,
            user=CLOUDSQL_USER,
            passwd=CLOUDSQL_PASSWORD,
            db='test3')

    # If the unix socket is unavailable, then try to connect using TCP. This
    # will work if you're running a local MySQL server or using the Cloud SQL
    # proxy, for example:
    #
    #   $ cloud_sql_proxy -instances=your-connection-name=tcp:3306
    #
    else:
        db = MySQLdb.connect(
            host='127.0.0.1', user=CLOUDSQL_USER, passwd=CLOUDSQL_PASSWORD)

    return db

def connect_to_cloudsql_sqlalchemy():
    connection_string = 'mysql+mysqldb://root:password@/test3?unix_socket=/cloudsql/digital-proton-146222:us-central1:test'
    engine = create_engine(connection_string)
    return engine




@app.route('/comment/<row_id>', methods=['GET', 'POST'])
def hello(row_id):
    db = connect_to_cloudsql()
    comment = request.form['comment'][:1000]

    cursor = db.cursor()
    cmd = 'UPDATE companies SET comment="{0}" WHERE id={1}'.format(comment, row_id)
    cursor.execute(cmd)
    resp = ''
    for r in cursor.fetchall():
        resp = resp + str(','.join(r))

    db.commit()
    db.close()

    return cmd

@app.route('/refresh_newco', methods=['GET'])
def refresh_newco():

    #Create the session
    engine = connect_to_cloudsql_sqlalchemy()
    session = sessionmaker()
    session.configure(bind=engine)
    s = session()

    connection = engine.connect()
    result = connection.execute('DROP TABLE IF EXISTS fc_leads')
    connection.close()

    get_all_list_items(NEWCO_LIST_ID, API_KEY, API_SECRET, engine = engine)
    return 'done'

@app.errorhandler(404)
def page_not_found(e):
    """Return a custom 404 error."""
    return 'Sorry, nothing at this URL.', 404
