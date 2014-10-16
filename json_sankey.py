import MySQLdb
#import csv
import sys
import json
from collections import defaultdict
import frogress
import time

def scrub(stmt):
    '''Helper function to scrub punctuation and whitespace from a string.
    - SO: 3247183/variable-table-name-in-sqlite'''
    return ''.join(c for c in stmt if c.isalnum())

def csv2db(credentials, name):
    '''Currently have data only in csv format. Put csv in mydmp.name
    `creator` variable very specific and not modularized :(
        - TODO - tablecreator()
    '''
    # connect
    (MYSQL_HOST, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DB) = credentials
    db = MySQLdb.connect(host=MYSQL_HOST, passwd=MYSQL_PASSWORD, 
                         user=MYSQL_USER, db=MYSQL_DB)
    cur = db.cursor()

    # strings
    name   = scrub(name)
    infile = name + '.csv'

    # commands
    destroyer = 'DROP TABLE IF EXISTS %s' % name
    creater = ('CREATE TABLE %s (UserID TEXT, conversion '
               'TINYINT, timetoconvert INT, duration_seen INT, events_total '
               'INT, events_before_cv INT, browser TINYINT, fractionBrowser '
               'FLOAT(10,7), OS TINYINT, fractionOS FLOAT(10,7), '
               'browser_at_cv TINYINT, os_at_cv TINYINT, dayofweek_at_cv '
               'TINYINT, timeofday_at_cv FLOAT(10,7))' % name)
    loader = ("LOAD DATA LOCAL INFILE '%s' INTO TABLE %s FIELDS "
              "TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' LINES TERMINATED "
              "BY '\n' IGNORE 1 LINES" % (infile, name))

    # execute & commit
    print 'Importing %s into the %s table.' % (infile, MYSQL_DB + '.' + name)
    cur.execute(destroyer)
    cur.execute(creater)
    cur.execute(loader)
    db.commit()
    cur.close()
    print 'Finished importing.'

def tablecreator(credentials, name):
    '''Create a string SQL command to set the `creator` command in csv2db.'''
    (MYSQL_HOST, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DB) = credentials
    # haven't written
    pass

def db2json(credentials, name, browsers, operating):
    '''Format data from MySQLdb to Sankey json file, 
    see sankeygreenhouse.json for an example.'''
    # connect
    (MYSQL_HOST, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DB) = credentials
    db = MySQLdb.connect(host=MYSQL_HOST, passwd=MYSQL_PASSWORD, 
                         user=MYSQL_USER, db=MYSQL_DB)
    cur = db.cursor()

    def repeatcmd(name, *args):
        '''Return int count value.
        Create the command by adding together strings.
        '''
        cmd = 'SELECT COUNT(*) FROM %s' % name
        n = len(args)

        if n/2: cmd += ' WHERE '
        for i in xrange(n): # this makes me unhappy
            if i % 2: continue
            if i != 0: cmd += 'and '
            cmd += '%s = %s ' % (args[i], args[i+1])

        cur.execute(cmd)
        return cur.fetchone()[0]

    # strings
    name    = scrub(name)
    outfile = name + '.json'

    # variables, defaults
    cv,  CV,  conversion,    outcv  = 'conversion',    '1', defaultdict(int), name + 'ncv.json'
    ncv, NCV, nonconversion, outncv = 'nonconversion', '0', defaultdict(int), name + 'cv.json'

    # distinguishing
    s = ' ' # ncv has addl variable s

    # os -> browser -> conversion
    print 'Converting data to links.'
    for b in frogress.bar(browsers):
        # browser to nonconversion
        nonconversion[browsers[b]+s, ncv] += repeatcmd(name, cv, NCV, 'browser', b)

        # browser to conversion
        conversion[browsers[b], cv] += repeatcmd(name, cv, CV, 'browser', b)

        # os to browser
        # differentiating by ncv/cv slows it down by 3 times
        for os in operating:
            # os to browser to nonconversion
            nonconversion[operating[os]+s+browsers[b][0], browsers[b]+s] += \
                            repeatcmd(name, 'OS', os, 'browser', b, cv, NCV)
            # os to browser to conversion
            conversion[operating[os]+browsers[b][0], browsers[b]] += \
                            repeatcmd(name, 'OS', os, 'browser', b, cv, CV)

    # write to the dictionary and json
    print '\nWriting nodes to json.'
    
    def nodemaker(links, extra=[]):
        '''Convert integer 2-keyed links dictionary to nodes list.
        Optional extra (SINGLE) parameter to add.'''
        nodes = [s for (s,t), v in links.items() if v > 0]
        nodes.append(extra)
        nodes = set(nodes)
        return [{"name":n} for n in nodes]

    nodesncv = nodemaker(nonconversion, ncv)
    nodescv  = nodemaker(conversion, cv)
    nodes    = nodesncv + nodescv

    print 'Writing links to json.'
    def linkmaker(links):
        '''Convert integer 2-keyed links dictionary to links list'''
        return [{"source":s, "target":t, "value":v} for (s,t), v in links.items() if v > 0]

    linksncv = linkmaker(nonconversion)
    linkscv  = linkmaker(conversion)
    links    = linksncv + linkscv

    def writejson(links, nodes, outfile):
        '''lol so much repeating and helper functions'''
        linksandnodes = {"links":links, "nodes":nodes}
        with open(outfile, 'w') as f:
            json.dump(linksandnodes, f)

    #writejson(linksncv, nodesncv, outncv)
    #writejson(linkscv, nodescv, outcv)
    writejson(links, nodes, outfile)

if __name__ == "__main__":
    credentials = ('localhost', 'root', 'kahasi', 'mydmp')
    name = 'profile'
    headers = ["UserID", "convert", "timetoconvert", "duration_seen", 
               "events_total", "events_before_cv", "browser", 
               "fractionBrowser", "OS", "fractionOS", "browser_at_cv", 
               "os_at_cv", "dayofweek_at_cv", "timeofday_at_cv"]
    browsers = { 1:'IE', 2:'IE', 8:'IE', 9:'FF', 10:'SAFARI', 11:'IE', 12:'IE', 
                 13:'CHROME', 14:'FF', 15:'FF', 16:'SAFARI', 17:'SAFARI', 
                 18:'SAFARI', 19:'SAFARI', 20:'IE', 21:'FF', 25:'IE', 
                 26:'SAFARI', 27:'SAFARI', 28:'SAFARI', 29:'IE', 30:'IE', 
                 5:'NN', 22:'OPERA', 24:'OPERA' }
    operating = { 1:'WINDOWS', 2:'WINDOWS', 3:'WINDOWS', 4:'WINDOWS', 
                  5:'WINDOWS', 6:'WINDOWS', 8:'WINDOWS', 9:'WINDOWS', 10:'iOS', 
                  11:'iOS', 12:'iOS', 13:'android', 14:'android', 17:'android', 
                  18:'android', 20:'android', 21:'android', 22:'WINDOWS', 
                  7:'MAC', 23:'MAC', 0:'Unknown', 19:'windows phone'}

    # import data to database
    #csv2db(credentials, name)
    
    # get data output
    t = time.clock()
    db2json(credentials, name, browsers, operating)
    print time.clock()-t, 's'
    
