
import sys
import simplejson
import optparse
import traceback
import time
import subprocess


def get_arg_parser():
    usage = '%prog [options] tagname'
    p = optparse.OptionParser(usage=usage)
    p.add_option('-H', '--host',
                 default="http://adswhy:9098", action='store',
                 help='Solr server location')
    p.add_option('-v', '--verbose',
                 default=False, action='store_true',
                 help='Operate in silent mode')
    p.add_option('-p', '--purge',
                 default=False, action='store_true',
                 help='Delete all docs before start')
    p.add_option('-o', '--offset',
                 default=0, action='store', type=int,
                 help='Skip the first X lines')
    p.add_option('-l', '--limit',
                 default=0, action='store', type=int,
                 help='Process maximux X lines (not including offset)')
    
    return p 


    
def run_cmd(cmd, silent=False, strict=True):
    try:
        if silent:
            code = subprocess.call(cmd, shell=True, stdout=subprocess.PIPE)
        else:
            code = subprocess.call(cmd, shell=True)
    except OSError:
        error('failed: %s' % cmd)
    else:
        if strict and code != 0:
            error('failed: %s' % cmd)
        return code    


def error(msq):  
    sys.stderr.write(msq)
    sys.exit(1)      
    



def normalize_json(json):
    # we must remove the copyfield and other sources of duplicate data
    for k in ['_version_', 'indexstamp', 'recid', 'keyword', 'keyword_facet', 'bibgroup_facet', 'data_facet', 
              'vizier_facet', 'all' ]:
        if k in json:
            del json[k]
    

def save_and_upload(options, recs):
    with open('tmp.json', 'w') as json:
        json.write('[')
        json.write(',\n'.join(recs))
        json.write(']')
    run_cmd("curl %s %s/solr/update -H 'Content-type:application/json' -d @tmp.json" % 
            (options.verbose and '' or '-s', options.host), silent=not options.verbose)
    
def commit(options):
    run_cmd("curl %s %s/solr/update?commit=true" % 
            (options.verbose and '' or '-s', options.host), silent=not options.verbose)

def purge(options):
    run_cmd("curl %s %s/solr/update?commit=true -H \"Content-Type: text/xml\" --data-binary '<delete><query>*:*</query></delete>'" % 
            (options.verbose and '' or '-s', options.host), silent=not options.verbose)
    
    
def main(argv):
    parser = get_arg_parser()
    options, args = parser.parse_args(argv)
    
    if options.purge:
        purge(options)
        
    print 'Starting import'
    now = time.time()
    
    offset = options.offset
    limit = options.limit
    
    overhead = 0
    i = j = 0
    recs = []
    for afile in args[1:]:
        fi = open(afile, 'r')
        for line in fi:
            overhead_start = time.time()
            line = line.strip()
            if line == '':
                continue
            i += 1
            if offset > 0 and i < offset:
                continue
            if limit > 0 and j == limit:
                break
            json = simplejson.loads(line)
            normalize_json(json)
            recs.append(simplejson.dumps(json))
            overhead += time.time() - overhead_start
            j += 1
            
            if j % 1000 == 0:
                save_and_upload(options, recs)
                recs = []
                print 'i: %s, done: %s, avg: %f, overhead: %s' % (i, j, (i / (time.time() - now)), overhead)
    if recs:
        save_and_upload(options, recs)
        commit(options)
    
    print 'Finished: %d recs in %s s.' % (j, time.time() - now,)
    print 'Overhead from processing json recs: %s s.' % (overhead,)
    

if __name__ == '__main__':
    main(sys.argv)