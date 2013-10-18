
import sys
import MySQLdb as mysql
import simplejson
import optparse
import getpass
import traceback
import time


def get_arg_parser():
    usage = '%prog [options] tagname'
    p = optparse.OptionParser(usage=usage)
    p.add_option('-i', '--create_index',
                 default=False, action='store_true',
                 help='Build indexes')
    p.add_option('-C', '--create_database',
                 default=False, action='store_true',
                 help='Create database (will ask you for a mysql root password')
    p.add_option('--drop',
                 default=False, action='store_true',
                 help='Drop data')
    p.add_option('-c', '--create_table',
                 default=False, action='store_true',
                 help='Create table')
    p.add_option('-d', '--database',
                 default="importtest", action='store',
                 help='Database name')
    p.add_option('-H', '--host',
                 default="localhost", action='store',
                 help='Localhost')
    p.add_option('-u', '--username',
                 default="test", action='store',
                 help='Username')
    p.add_option('-p', '--password',
                 default="test", action='store',
                 help='Password')
    p.add_option('--port',
                 default="3306", action='store',
                 help='Port')
    p.add_option('-t', '--table',
                 default="bigtable", action='store',
                 help='Name of the table that stores data')
    return p 


def create_database(options):
    
    assert options.database != '' 
    
    print 'We will create a database %s and user %s in mysql' % (options.database, options.username)
    rootpass = getpass.getpass('Mysql root password:')
    conn = make_connection(options.host, 'root', rootpass, '')
    cur = conn.cursor()
    
    if options.drop:
        cur.execute("""DROP DATABASE IF EXISTS %(database)s""" % options.__dict__)
        
    
                
    cur.execute("""CREATE DATABASE IF NOT EXISTS %(database)s
                DEFAULT CHARACTER SET = utf8
                """ % options.__dict__)
    
    #cur.execute("""CREATE USER '%(username)s'@'%(host)s' IDENTIFIED BY '%(password)s'
    #            """ % options.__dict__)
    
    # this will create the user if not there
    cur.execute("""GRANT ALL ON %(database)s.* TO '%(username)s'@'%(host)s IDENTIFIED BY '%(password)s'
                """ % options.__dict__)
    cur.execute("FLUSH PRIVILEGES")
    
    
    
    
def check_options(options):
    
    if options.create_database:
        create_database(options)
    
    # register normal host connection
    conn = get_connection(options.host, options.username, options.password, options.database)
    cur = conn.cursor()
    
    check_table_exists(options)
    
    if options.create_index: 
        create_indexes(options)
    
def create_indexes(options):
    """
    So in principle it is possible to normalize data and have them indexed separately,
    such as MongoDB 'multi-valued' index - by using stored procedures; there even is 
    a special project that could be adapted for it
    
    http://code.google.com/p/inverted-index/source/browse/trunk/sql/sp_indexString.sql
    
    postgres seems to have some support for this:
    http://stackoverflow.com/questions/4058731/can-postgresql-index-array-columns
    http://www.postgresql.org/docs/9.1/interactive/hstore.html#AEN133006
    """
    
    conn = get_connection()
    cur = conn.cursor()
    
    #cur.execute("SELECT count(*) FROM INFORMATION_SCHEMA.STATISTICS WHERE table_name = '%(table)s'" % 
    #               options.__dict__)
    #if cur.fetchone()[0] > 1:
    #    print 'Indexes already defined'
    #    return
    
    cur.execute("SHOW COLUMNS FROM `%(table)s`" % options.__dict__)
    for col in cur.fetchall():
        if col[3] != '':
            continue # already has index
        
        if col[0] in ['abstract', 'full', 'pubdate_sort', 'first_author', 'first_author_norm', 'first_author_surname',
                      'indexstamp', 'author_surname']:
            continue
        if '_facet' in col[0]:
            continue 
        
        if 'text' in col[1] or 'blob' in col[1]:
            cur.execute("CREATE INDEX `%(column)s` ON `%(table)s` (`%(column)s`(512))" % {'table': options.table, 'column': col[0]})
            
            #cur.execute("ALTER TABLE `%(table)s` ADD INDEX `%(column)s` (%(column)s)" % 
            #            {'table': options.table, 'column': col[0]})
        else: 
            cur.execute("CREATE INDEX `%(column)s` ON `%(table)s` (`%(column)s`)" % {'table': options.table, 'column': col[0]})
            #cur.execute("ALTER TABLE `%(table)s` ADD INDEX `%(column)s` (%(column)s)" % 
            #            {'table': options.table, 'column': col[0]})    
    
    print 'Crated indexes on tbl: %s' % options.table
    

def check_table_exists(options):
    conn = get_connection()
    cur = conn.cursor()
    
    if options.drop:
        cur.execute("DROP TABLE IF EXISTS `%(table)s`" % options.__dict__)
        print 'Dropped: %s' % options.table
    
    cur.execute("""
      CREATE TABLE IF NOT EXISTS %(table)s (
        `bibcode` VARCHAR(19) PRIMARY KEY,
        `ack` TEXT CHARACTER SET utf8 COLLATE utf8_general_ci NULL,
        `aff` TEXT CHARACTER SET utf8 COLLATE utf8_general_ci NULL,
        `alternate_title` VARCHAR(255)  CHARACTER SET utf8 COLLATE utf8_general_ci NULL,
        `author` VARCHAR(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL,
        `author_surname` VARCHAR(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL,
        `author_facet` VARCHAR(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL,
        `author_facet_hier` VARCHAR(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL,
        `first_author` VARCHAR(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL,
        `first_author_norm` VARCHAR(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL,
        `first_author_surname` VARCHAR(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL,
        `author_norm` VARCHAR(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL,
        `bibgroup` VARCHAR(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL,
        `bibgroup_facet` VARCHAR(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL,
        `bibstem` VARCHAR(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL,
        `bibstem_facet` VARCHAR(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL,
        `citation_count` INT,
        `cite_read_boost` FLOAT,
        `comment` TEXT CHARACTER SET utf8 COLLATE utf8_general_ci NULL,
        `data` VARCHAR(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL,
        `data_facet` VARCHAR(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL,
        `copyright` VARCHAR(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL,
        `database` VARCHAR(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL,
        `date` VARCHAR(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL,
        `doi` VARCHAR(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL,
        `email` VARCHAR(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL,
        `facility` VARCHAR(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL,
        `first_author_facet_hier` VARCHAR(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL,
        `grant` VARCHAR(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL,
        `grant_facet_hier` VARCHAR(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL,
        `id` VARCHAR(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL,
        `identifier` VARCHAR(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL,
        `indexstamp` VARCHAR(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL,
        `ids_data` TEXT CHARACTER SET utf8 COLLATE utf8_general_ci NULL,
        `isbn` VARCHAR(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL,
        `issn` VARCHAR(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL,
        `issue` VARCHAR(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL,
        `keyword` VARCHAR(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL,
        `keyword_facet` VARCHAR(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL,
        `keyword_norm` VARCHAR(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL,
        `lang` VARCHAR(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL,
        `links_data` VARCHAR(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL,
        `page` VARCHAR(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL,
        `property` VARCHAR(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL,
        `pub` VARCHAR(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL,
        `pub_raw` VARCHAR(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL,
        `pubdate` VARCHAR(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL,
        `pubdate_sort` VARCHAR(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL,
        `read_count` INT,
        `reader` TEXT CHARACTER SET utf8 COLLATE utf8_general_ci NULL,
        `recid` INT,
        `reference` TEXT CHARACTER SET utf8 COLLATE utf8_general_ci NULL,
        `thesis` VARCHAR(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL,
        `title` VARCHAR(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL,
        `vizier` VARCHAR(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL,
        `vizier_facet` VARCHAR(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL,
        `volume` VARCHAR(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL,
        `year` VARCHAR(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL,
        `abstract` TEXT CHARACTER SET utf8 COLLATE utf8_general_ci NULL,
        `full` TEXT CHARACTER SET utf8 COLLATE utf8_general_ci NULL
        ) ENGINE=innodb
    """ % options.__dict__)
    

_conn = []
def get_connection(*args):
    if len(_conn) > 0:
        return _conn[0]
    _conn.append(make_connection(*args))
    return _conn[-1]

def make_connection(host, username, password, database):
    
    try:
        con = mysql.connect(host, username, password, database, charset='utf8');
        cur = con.cursor()
        cur.execute("SELECT VERSION()")
        ver = cur.fetchone()
        print "Database version : %s " % ver
        cur.execute("SET autocommit = 1")
        print 'We are in autocommit=1 mode'
        cur.execute("SET NAMES 'utf8'")
        cur.execute("show variables like 'char%'")
        print cur.fetchall()
        return con
        
    except mysql.Error, e:
        error("Error %d: %s" % (e.args[0],e.args[1]))
        

def error(msq):  
    sys.stderr.write(msq)
    sys.exit(1)      
    

def create_record(table, json):
    conn = get_connection()
    cur = conn.cursor()
    colnames = []
    values = []
    qmarks = []
    for k,v in json.items():
        colnames.append('`%s`' % k)
        values.append(v)
        qmarks.append('%s')
        
    cur.execute("""INSERT INTO %s( %s ) VALUES ( %s )""" % (table, ','.join(colnames),
                                                                   ','.join(qmarks)), values)

def update_record(table, json):
    conn = get_connection()
    cur = conn.cursor()
    values = []
    bibcode = None
    qmarks = []
    for k,v in json.items():
        if k == 'bibcode':
            bibcode = v
        else:
            qmarks.append('%s')
            values.append("`%s`='%s'" % (k,mysql.escape_string(v)))
    cur.execute("""UPDATE %s SET %s WHERE `bibcode`='%s' """ % (table, ', '.join(values), bibcode))

def normalize_json(json):
    if '_version_' in json:
        del json['_version_']
        
    for k,v in json.items():
        if isinstance(v, unicode):
            json[k] = v.encode('utf8')
        elif not isinstance(v, basestring):
            if isinstance(v, list):
                json[k] = ' | '.join([x.encode('utf8') for x in v])
            elif isinstance(v, ()):
                json[k] = ' | '.join([x.encode('utf8') for x in v])
            else:
                json[k] = str(v) 

def insert_record(tblname, json):
    normalize_json(json)

    try:
        create_record(tblname, json)
    except mysql.IntegrityError, e:
        update_record(tblname, json)
    except Exception, e:
        print 'Baaaad things happening!!!!'
        traceback.print_stack()
    

def show_stats(options):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM %(table)s" % options.__dict__)
    print 'Number of recs: %s' % cur.fetchone()[0]
    
    #cur.execute("SELECT * FROM %(table)s" % options.__dict__)
    #print cur.fetchall()
    
    
def main(argv):
    parser = get_arg_parser()
    options, args = parser.parse_args(argv)
    
    check_options(options)
    
    tblname = options.table
    
    print 'Starting import'
    show_stats(options)
    now = time.time()
    
    i = 0
    for afile in args[1:]:
        fi = open(afile, 'r')
        for line in fi:
            line = line.strip()
            if line == '':
                continue
            json = simplejson.loads(line)
            insert_record(tblname, json)
            i += 1
            if i % 1000 == 0:
                print 'done: %s, avg: %f' % (i, (i / (time.time() - now))) 
    
    print 'Finished: %s s.' % (time.time() - now,)
    show_stats(options)
    

if __name__ == '__main__':
    main(sys.argv)