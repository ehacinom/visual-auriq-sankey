import MySQLdb

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
    creator = ('CREATE TABLE %s (UserID TEXT, conversion '
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
    cur.execute(creator)
    cur.execute(loader)
    db.commit()
    cur.close()
    print 'Finished importing.'

def tablecreator(credentials, name):
    '''Create a string SQL command to set the `creator` command in csv2db.'''
    (MYSQL_HOST, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DB) = credentials
    # haven't written
    pass


if __name__ == "__main__":
    # import data to database
    credentials = ('localhost', 'root', 'kahasi', 'mydmp')
    name = 'profile'
    csv2db(credentials, name)