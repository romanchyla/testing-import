'''
Simple script given a dictionary of recids
change theit modification date inside
Invenio
'''

import os
import json
import MySQLdb

invenio_data_file = 'toupdaterecs.json'
invenio_host='adsx'
invenio_user='invenio'
invenio_pass= os.environ['INVENIO_DB_PASS']
invenio_db='inveniolive'
inveniodb = MySQLdb.connect(invenio_host, invenio_user, invenio_pass, invenio_db, charset='utf8');

if not os.path.exists(invenio_data_file):
    exit('Cannot see: %s' % invenio_data_file)
    

data = json.load(open(invenio_data_file, 'r'))

answer = raw_input('We are about to change modification date for %d records. Do you want that? (yes/no):' % len(data))

if answer == 'yes':
    cur = inveniodb.cursor()
    i = 0
    for k,v in data.items():
        #cur.execute("SELECT * from bibrec WHERE id=%d" % v)
        #print list(cur.fetchall())
        cur.execute("UPDATE bibrec SET modification_date=NOW() WHERE id=%d" % v) # i know, inefficient...
        #cur.execute("SELECT * from bibrec WHERE id=%d" % v)
        #print list(cur.fetchall())
        if i % 10000 == 0:
            print 'done:', i
        i += 1
    

