import os
from time import time

#import cloudstorage as gcs
from flask import Flask, request
from google.appengine.api import taskqueue
from google.cloud import storage
import MySQLdb
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from load_sfiq_fc_leads import get_all_list_items
from sp_util import format_domain

# Note: We don't need to call run() since our application is embedded within
# the App Engine WSGI application server.

# These environment variables are configured in app.yaml.
CLOUDSQL_CONNECTION_NAME = os.environ.get('CLOUDSQL_CONNECTION_NAME')
CLOUDSQL_USER = os.environ.get('CLOUDSQL_USER')
CLOUDSQL_PASSWORD = os.environ.get('CLOUDSQL_PASSWORD')

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
    connection_string = (
        'mysql+mysqldb://root:password@/test3?unix_socket='
        '/cloudsql/digital-proton-146222:us-central1:test')
    engine = create_engine(connection_string)
    return engine

@app.route('/comment/<row_id>', methods=['GET', 'POST'])
def hello(row_id):
    db = connect_to_cloudsql()
    comment = request.form['comment'][:1000]

    cursor = db.cursor()
    cmd = '''
          UPDATE companies SET comment="{0}" WHERE id={1}
          '''.format(comment, row_id)
    cursor.execute(cmd)
    resp = ''
    for r in cursor.fetchall():
        resp = resp + str(','.join(r))

    db.commit()
    db.close()

    return cmd

@app.route('/refresh_newco', methods=['GET'])
def refresh_newco():
    task = taskqueue.add(
        url='/update_fc_leads',
        target='worker')
    return 'done'

@app.route('/', methods=['GET', 'POST'])
def upload_files():
    if request.method == 'GET':
        # Serve static upload form
        return '''
            <!doctype html>
            <form action="#" method="post" enctype="multipart/form-data">
                <input type="file" name="file"/>
                <input type="submit" value="Upload" />
            </form>
            '''
    if request.method == 'POST':
        my_file = request.files['file']

        storage_client = storage.Client()
        bucket = storage_client.get_bucket('ds5000')
        blob = bucket.blob(my_file.filename)
        blob.upload_from_file(my_file, size=my_file.content_length)
        task = taskqueue.add(
            url='/update_pb_rounds',
            target='worker',
            params={'filename': my_file.filename})


        #filename = '/ds5000/' + my_file.filename
        #new_file = gcs.open(filename)
        return ('File {} uploaded.'.format(
            my_file.filename))

@app.errorhandler(404)
def page_not_found(e):
    """Return a custom 404 error."""
    return 'Sorry, nothing at this URL.', 404
