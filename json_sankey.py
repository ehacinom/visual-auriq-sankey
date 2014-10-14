import MySQLdb
#import csv
#import sys
import json
import frogress

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
    pass

def db2json(credentials, name, browsers, operating):
    '''Format data from MySQLdb to Sankey json file, 
    see sankeygreenhouse.json for an example.'''
    # connect
    (MYSQL_HOST, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DB) = credentials
    db = MySQLdb.connect(host=MYSQL_HOST, passwd=MYSQL_PASSWORD, 
                         user=MYSQL_USER, db=MYSQL_DB)
    cur = db.cursor()

    def repeatcmd(name, var1, val1, var2, val2):
        '''Return string value'''
        cmd = 'SELECT COUNT(*) FROM %s WHERE %s = %s and %s = %s'
        cmd = cmd % (name, var1, val1, var2, val2)
        cur.execute(cmd)
        return str(cur.fetchone()[0])

    # strings
    name    = scrub(name)
    outfile = name + '.json'

    # variables
    cv,  CV  = 'conversion', '1'
    ncv, NCV = 'nonconversion', '0'

    # defaults
    links, nodes = [], []

    # links between nodes
    nodes.append({"name": cv})
    nodes.append({"name": ncv})
    print 'Calculating browser to conversion links.'
    for b in frogress.bar(browsers):
        # browser to conversion and nonconversion
        var2 = 'browser'
        link = {"source": browsers[b],
                "target": ncv,
                "value" : repeatcmd(name, cv, NCV, var2, b) }
        links.append(link)
        link = {"source":browsers[b],
                "target":cv,
                "value":repeatcmd(name, cv, CV, var2, b) }
        links.append(link)
        
        nodes.append({"name":browsers[b]})
    var1 = 'browser'
    print 'Calculating OS to conversion and browser links.'
    for os in frogress.bar(operating):
        # os to conversion and nonconversion
        var2 = 'OS'
        link = {"source": operating[os],
                "target": cv,
                "value" : repeatcmd(name, cv, CV, var2, os) }
        links.append(link)
        link = {"source": operating[os],
                "target": ncv,
                "value" : repeatcmd(name, cv, NCV, var2, os) }
        links.append(link)
        
        nodes.append({"name":operating[os]})

        # os to browser
        for b in browsers:
            link = {"source": operating[os],
                    "target": browsers[b],
                    "value" : repeatcmd(name, var2, os, var1, b)}
            links.append(link)

    # write to the dictionary and json
    print 'Writing to json.'
    linksandnodes = {"links":links, "nodes":nodes}
    with open(outfile, 'w') as f:
        json.dump(linksandnodes, f)

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
    csv2db(credentials, name)
    
    # get data output
    db2json(credentials, name, browsers, operating)
    
