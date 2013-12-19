
"""
throw away script to find differences in mongo
vs solr index and trigger reindexing them
through invenio

Warning, this script will eat lot of RAM as it
needs to load 3x10M bibcodes. Makes no sense to
query for that data

this is not a program, just a heavily edited file
with instructions (should have been in ipython
notebook)
"""

import pymongo
import sys
import os
import requests
import json
import MySQLdb
import pprint
import time
import threading


"""
Start exporting data from solr; we can let it run 
and get back to it when ready. The idea is we want
to compare Mongo against SOLR; so we first retrieve
documents that should have data of MongoDB provenance
"""    

solr_data_file = 'solrdata.json'
solr_endpoint = 'http://adswhy:9011/solr/batch'
batch_query = {'command': 'dump-index', 'wt':'json', 'indent':'true'}
batch_query['fields'] = 'bibcode' # comma separated list of values to get back
batch_query['q'] = 'body:*' # get all docs that have fulltext indexed
#batch_query['q'] = 'citation:*' # get all docs that have citations
#batch_query['q'] = 'recid:[10 TO 50]' # just for testing


threads_to_check = []
solrdata = {}

if os.path.exists(solr_data_file):
    print 'loading cached data from: %s' % solr_data_file
    solrdata = json.load(open(solr_data_file, 'r'))
else:
    
    rsp = requests.get(solr_endpoint, params=batch_query)
    rsp = json.loads(rsp.text)
    jobid = rsp['jobid']

    print 'registered new solr job: %s' % jobid
    
    rsp = requests.get(solr_endpoint, params={'command': 'start', 'wt': 'json'})
    
    rsp = requests.get(solr_endpoint, params={'command': 'status', 'jobid': jobid, 'wt': 'json'})
    rsp = json.loads(rsp.text)
    if rsp['status'] != 'busy':
        print 'Unknown status of the job - sorl is maybe busy?'
        pprint.pprint(rsp)
        
    
    # for debugging
    
    """
    Now register a method that will check the job
    and download the data when ready
    """
    def get_solr_data(solr_endpoint, jobid, solrdata, dump_to_file):
        while True:
            rsp = requests.get(solr_endpoint, params={'command': 'status', 'jobid': jobid, 'wt': 'json'})
            rsp = json.loads(rsp.text)
            i = 0
            if 'job-status' in rsp and rsp['job-status'] == 'finished':
                r = requests.get(solr_endpoint, params={'command': 'get-results', 'jobid': jobid}, 
                                 stream=True)
                with open(dump_to_file, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=1024): 
                        if chunk: # filter out keep-alive new chunks
                            f.write(chunk)
                            f.flush()
                break
            else:
                time.sleep(5)
                if i % 10 == 0:
                    print 'Strill waiting for solr: %s' % jobid
                i += 1
        tmp = json.load(open(dump_to_file, 'r'))
        solrdata.update(tmp) 
    
    
    solrt = threading.Thread(target=get_solr_data, args=(solr_endpoint, jobid, solrdata, solr_data_file))
    solrt.start()
    threads_to_check.append(solrt)


mongo_host = 'adsx'
mongo_pass = os.environ['MONGO_PASS']
mongo_user = 'adsdata'
mongo_database = 'adsdata'
mongo_collection = 'docs'

# get connection to mongo
client = pymongo.MongoClient(mongo_host)
db = getattr(client, mongo_database)
db.authenticate(mongo_user, mongo_pass)
coll = db[mongo_collection]

 


"""
Harvest docs from mongo - their bibcodes;
it is important to change the mongo_query
"""
mongo_query = {'_id': {'$exists': True}} # find everything
mongo_query = {'full': {'$exists': True}} # find everything with fulltext
#mongo_query = {'norm_cites': {'$exists': True}} # find everything with classic factor

fields_to_harvest = {'_id': True} #or None to get everything
fields_to_keep = {}

mongo_data_file = 'mongodata.json'
mongodata = {}

def harvest_mongo(coll, mongo_query, fields_to_harvest, mongodata, dump_to_file):
    i = 0
    for doc in coll.find(mongo_query, fields_to_harvest): #.limit(5): # for testing
        docid = doc.pop('_id')
        if fields_to_keep:
            for k,v in doc.items():
                if k not in fields_to_keep:
                    del doc[k]
            mongodata[docid] = doc
        else:
            mongodata[docid] = True
        if i % 100000 == 0:
            print 'Mongo harvest: %s' % i
        i += 1
    json.dump(mongodata, open(dump_to_file, 'w'))
    
if os.path.exists(mongo_data_file):
    print 'loading cached data from: %s' % mongo_data_file
    mongodata = json.load(open(mongo_data_file, 'r'))
else:
    print 'Harvesting data from mongo: %s' % fields_to_harvest
    mongot = threading.Thread(target=harvest_mongo, args=(coll, mongo_query, fields_to_harvest, mongodata, mongo_data_file))
    mongot.start()
    threads_to_check.append(mongot)


"""
We also need to know mapping from bibcodes into recids
as stored in Invenio (theoretically, we could get that
from solr; but if we also want to check what is NOT 
present in solr, then we have to get it from Invenio
"""

invenio_data_file = 'inveniodata.json'
invenio_host='adsx'
invenio_user='invenio'
invenio_pass= os.environ['INVENIO_DB_PASS']
invenio_db='inveniolive'
inveniodb = MySQLdb.connect(invenio_host, invenio_user, invenio_pass, invenio_db, charset='utf8');
inveniodb2 = MySQLdb.connect(invenio_host, invenio_user, invenio_pass, invenio_db, charset='utf8');


def harvest_invenio_bibcodes(inveniodb, inveniodata, dump_to_file, deletes=False):
    cur = inveniodb.cursor()
    i = 0
    if deletes:
        cur.execute('select t2.id_bibrec,t2.id_bibrec from bib98x as t1 ' +
                    'inner join bibrec_bib98x as t2 ON t1.id=t2.id_bibxxx ' +
                    'WHERE t2.id_bibxxx=(select id from bib98x where value="DELETED") ' +
                    'order by t2.id_bibrec desc' # + ' limit 10'
                    )
        #cur.execute('SELECT id_bibrec, id_bibrec FROM bibrec_bib98x WHERE ' +
        #            'id_bibxxx=(select id from bib98x where value="DELETED"')
    else:
        cur.execute('select t1.value,t2.id_bibrec from bib97x as t1 ' +
                           'inner join bibrec_bib97x as t2 ON t1.id=t2.id_bibxxx ' +
                           'WHERE t1.tag="970__a" order by t2.id_bibrec desc'  # + ' limit 10'
                           )
    for rec in cur.fetchall():
        inveniodata[rec[0]] = int(rec[1])
        if i % 100000 == 0:
            'Invenio - finished: %d' % i
        i += 1
    json.dump(inveniodata, open(dump_to_file, 'w'))
    cur.close()
        
    


inveniodata = {}
if os.path.exists(invenio_data_file):
    print 'loading cached data from: %s' % invenio_data_file
    inveniodata = json.load(open(invenio_data_file, 'r'))
else:
    print 'harvesting invenio'
    inveniot = threading.Thread(target=harvest_invenio_bibcodes, args=(inveniodb, inveniodata, invenio_data_file))
    inveniot.start()
    threads_to_check.append(inveniot)

invenio_data_file = 'inveniodatadels.json'
inveniodels = {}
if os.path.exists(invenio_data_file):
    print 'loading cached data from: %s' % invenio_data_file
    inveniodels = json.load(open(invenio_data_file, 'r'))
else:
    print 'harvesting invenio'
    inveniotd = threading.Thread(target=harvest_invenio_bibcodes, args=(inveniodb2, inveniodels, invenio_data_file, True))
    inveniotd.start()
    threads_to_check.append(inveniotd)    



# now wait it out
for t in threads_to_check:
    if t is not None or not t.isAlive():    
        t.join()



# create a huge map from bibcode into integers
# it will be lowercased, because solr is lowercase
bigbib = {}
i = 0
for rec in solrdata['data']:
    if rec['bibcode'][0] not in bigbib:
        bigbib[rec['bibcode'][0]] = i
        i += 1
for bibcode in mongodata.keys():
    bibcode = bibcode.lower()
    if bibcode not in bigbib:
        bigbib[bibcode] = i
        i += 1
for bibcode in inveniodata.keys():
    bibcode = bibcode.lower()
    if bibcode not in bigbib:
        bigbib[bibcode] = i
        i += 1
    

print 'created bigmap'

# turn bibcodes into sets of integers
solrset = set([bigbib[x['bibcode'][0]] for x in solrdata['data']])
solrdata = None
inveset = set([bigbib[x.lower()] for x in inveniodata.keys()])
mongset = set([bigbib[x.lower()] for x in mongodata.keys()])
mongodata = None

with open('bigmap.data', 'w') as bm:
    for k,v in bigbib.items():
        bm.write('%s\t%s\n' % (k,v))
bigbib = None 
print 'released memory'

# deleted recids (discover their ids)
deleset = set()
for k,v in inveniodata.items():    
    if str(v) in inveniodels:
        deleset.add(k.lower())


# remove deletes bibcodes from invenio set
inveset = inveset - deleset    

"""
Now find what is missing in SOLR but is present in Mongo
and Invenio etc...
"""

print "--------"
print "Solr query: %s" % batch_query['q']
print "Mongo query: %s" % mongo_query
print "Invenio query: all recs"

print "--------"

print 'Invenio #recs     : %s' % len(inveset)
print 'Invenio #dels     : %s' % len(deleset)
print 'Mongo #recs       : %s' % len(mongset)
print 'Solr #recs        : %s' % len(solrset)


present_in_solr_missing_in_invenio = solrset.difference(inveset)
present_in_solr_missing_in_mongo = solrset.difference(mongset)

present_in_mongo_missing_in_invenio = mongset.difference(inveset)
present_in_mongo_missing_in_solr = mongset.difference(solrset)

present_in_invenio_missing_in_solr = inveset.difference(solrset)
present_in_invenio_missing_in_mongo = inveset.difference(mongset)

print "--------"

print 'present_in_solr_missing_in_invenio   :', len(present_in_solr_missing_in_invenio)
print 'present_in_solr_missing_in_mongo     :', len(present_in_solr_missing_in_mongo)

print 'present_in_mongo_missing_in_invenio  :', len(present_in_mongo_missing_in_invenio)
print 'present_in_mongo_missing_in_solr     :', len(present_in_mongo_missing_in_solr)

print 'present_in_invenio_missing_in_solr   :', len(present_in_invenio_missing_in_solr)
print 'present_in_invenio_missing_in_mongo  :', len(present_in_invenio_missing_in_mongo)

print "--------"

print 'mongo & present_in_solr_missing_in_invenio :', len(mongset & present_in_solr_missing_in_invenio)
print 'invenio & present_in_solr_missing_in_mongo :', len(inveset & present_in_solr_missing_in_mongo)

print 'solr & present_in_mongo_missing_in_invenio :', len(solrset & present_in_mongo_missing_in_invenio)
print 'invenio & present_in_mongo_missing_in_solr :', len(inveset & present_in_mongo_missing_in_solr)

print 'mongo & present_in_invenio_missing_in_solr :', len(mongset & present_in_invenio_missing_in_solr)
print 'solr & present_in_invenio_missing_in_mongo :', len(solrset & present_in_invenio_missing_in_mongo)


# now find out which records need updating in invenio
to_update_ids = {}.fromkeys(inveset & present_in_mongo_missing_in_solr)

# but first release memory
present_in_solr_missing_in_invenio = present_in_solr_missing_in_mongo = present_in_mongo_missing_in_invenio = None
present_in_mongo_missing_in_solr = present_in_invenio_missing_in_solr = present_in_invenio_missing_in_mongo = None

# collect bibcodes based on their id (they are lowercase at this stage)
bibcodes_to_update = {}
with open('bigmap.data', 'r') as bm:
    for line in bm:
        bibcode, bid = line.split()
        if int(bid) in to_update_ids:
            to_update_ids[int(bid)] = bibcode

# now we must turn bibcodes into recids
to_update_ids = {}.fromkeys(to_update_ids.values())
to_update_bibcodes = {}
for k,v in inveniodata.items():
    if k.lower() in to_update_ids:
        to_update_bibcodes[k] = v


print "--------"
print 'writing list of recids that should be updated in Invenio'    
json.dump(to_update_bibcodes, open('toupdaterecs.json', 'w'))
