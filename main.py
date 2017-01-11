import os
from time import time
import traceback

from flask import Flask, request
from google.appengine.api import taskqueue
from google.cloud import storage
import MySQLdb
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from worker import connect_to_cloudsql_sqlalchemy

# Initialize sql alchemy declarative base
Base = declarative_base()

# initialize flask
app = Flask(__name__)
app.config['DEBUG'] = True

class Comment(Base):
    __tablename__ = 'comments'
    id = Column(Integer, primary_key=True, nullable=False)
    company_name = Column(String(255))
    company_website = Column(String(255))
    comment = Column(String(1023))
    interest = Column(String(255))

def connect_to_cloudsql_sqlalchemy_bak():
    connection_string = (
        'mysql+mysqldb://root:password@/test3?unix_socket='
        '/cloudsql/digital-proton-146222:us-central1:test')
    engine = create_engine(connection_string)
    return engine

@app.route('/comment', methods=['GET', 'POST'])
def set_comment():
    comment = request.form['comment'][:1000]
    row_id = request.form['row_id']

    # Create engine
    engine = connect_to_cloudsql_sqlalchemy()
    Base.metadata.create_all(engine)

    # Get matching company
    connection = engine.connect()
    result = connection.execute('SELECT * FROM companies WHERE id={0}'
        .format(row_id))
    connection.close()

    for company in result:
        connection = engine.connect()
        result = connection.execute(
            '''
            UPDATE comments
            SET comment='{0}'
            WHERE company_name='{1}'
                AND company_website='{2}'
            '''.format(comment,
                       company.company_name,
                       company.company_website))
        if result.rowcount == 0:
            result = connection.execute(
                '''
                INSERT INTO comments (company_name, company_website, comment)
                VALUES ('{0}', '{1}', '{2}')
                '''.format(company.company_name,
                           company.company_website,
                           comment))
        connection.close()

    return row_id + ', ' + comment

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
            Pitchbook
            <form action="/load_file"
                    method="post" enctype="multipart/form-data">
                <input type="hidden" name="destination" value="/load_pb_rounds"/>
                <input type="file" name="file"/>
                <input type="submit" value="Upload" />
            </form>
            <br/>
            Second Measure
            <form action="/load_file"
                    method="post" enctype="multipart/form-data">
                <input type="hidden" name="destination" value="/load_sm_csv"/>
                <input type="file" name="file"/>
                <input type="submit" value="Upload" />
            </form>
            '''

@app.route('/load_file', methods=['POST'])
def begin_update_pb_rounds():
    my_file = request.files['file']

    storage_client = storage.Client()
    bucket = storage_client.get_bucket('ds5000')
    blob = bucket.blob(my_file.filename)
    blob.upload_from_file(my_file, size=my_file.content_length)
    task = taskqueue.add(
        url=request.form['destination'],
        target='worker',
        params={'filename': my_file.filename})

    return ('File {} uploaded.'.format(
        my_file.filename))

@app.route('/begin_transform_sm_growth', methods=['GET'])
def begin_transform_sm_growth():
    task = taskqueue.add(
        url='/transform_sm_growth',
        target='worker')
    return 'done'

@app.errorhandler(404)
def page_not_found(e):
    """Return a custom 404 error."""
    return 'Sorry, nothing at this URL.', 404
