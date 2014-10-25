import MySQLdb
import _mysql_exceptions
#http://sourceforge.net/p/mysql-python/discussion/70461/thread/21fb6268/
import os
import csv
import sys

import json
from collections import defaultdict
import frogress
import multiprocessing as mp


# TODO TODO TODO TODO
# MODULES:
# note, rename table columns afterwards
# http://stackoverflow.com/questions/4002340/
# note, changing types afterwards? necessary in this package!!


class JSONfun(object):
    '''Getting database data into the correct json format.
    
    USAGE SCRIPT
    ============
    
    from csv2json import JSONfun  
    credentials = ($MYSQL_HOST, $MYSQL_USER, $MYSQL_PASSWORD, $MYSQL_DB)  
    name = '$NAME'  
    data = JSONfun(crednetials, name)
      
    EXPLANATION
    ===========
    MySQL credentials are self-explanatory.  
    The name is the name, sans '.csv', of the file you are working with.  
      
    For example, if your name = `profile`, data from profile.csv would be
    put into the $MYSQL_DB.profile table and saved to profile.json.
    
    
    '''

    def __init__(self, credentials, name):
        '''connect to database
        
        :param: credentials - (HOST, USER, PASSWORD, DB) for MySQLdb
        :param: name - file/table to play with
        '''
        # connect
        self.credentials = credentials
        self.db = MySQLdb.connect(host   = self.credentials[0], 
                                  user   = self.credentials[1], 
                                  passwd = self.credentials[2], 
                                  db     = self.credentials[3])
        self.cur = self.db.cursor()

        # checking the csv file exists in your directory
        self.csvfile = name + '.csv'
        if not os.path.isfile(self.csvfile):
            print 'The file %s is not in this directory.' % csvfile
            sys.exit()

        # checking the name
        self.name = self.scrub(str(name))
        if self.name != name:
            print 'Warning!'
            print 'name = %s is not alphanumeric and has been replaced by %s' \
                   % (str(name), self.name)

        # database dump from csv
        if self.csv2db():
            print 'Data successfully imported from %s to database table %s'\
                   % (self.csvfile, self.name)
        else:
            print 'Using data stored in database table %s' % self.name

        # we'll move db --> json in individual function calls

    def scrub(self, stmt):
        '''Helper function to scrub punctuation and whitespace from a string.
        Allow underscore and dash and $ if necessary.'''
        return ''.join(c for c in stmt if c.isalnum() or c in '_-$')

    def askthrice(self, question):
        '''ask a question 0/1 question three times
        default 0'''
        ask, exitval = 0, 3
        while ask < exitval:
            ans = raw_input(question).strip()
            if ans == '1' or ans == '0':
                ask = exitval
            else: 
                ans = '0'
                ask += 1
        return int(ans)

    def checktable(self):
        '''check for preexisting table with the same name
            if yes, ask to re-drop data into database (0,1)
            if no, drop data into database (1)
        '''
        check = "SHOW TABLES LIKE '%s'" % self.name
        self.cur.execute(check)
        if self.cur.fetchone():
            print 'You have a pre-existing table of the name \'%s\'.' % self.name
            question = 'Use original data (0) or new data from the csv (1)? '
            return self.askthrice(question)
        return 1

    def tablecreator(self):
        '''create a string SQl command to set up the `creator` command
        used in csv2db()
        
        All column types are strings :p
        '''
        r = csv.reader(open(self.csvfile, 'rb'))
        header = r.next()
        self.n = len(header)

        # check header is valid
        valid_columns = 0
        for column in header:
            num_chars = sum(c.isalpha() for c in column)
            if num_chars > 0:
                valid_columns += 1
        if valid_columns == self.n:
            self.cols = header
        else:
            self.cols = ['C'+str(i) for i in xrange(self.n)]

        # okay, iteratively create this SQL command `creator`
        middle = ' TEXT, '.join(self.cols)
        creator = 'CREATE TABLE %s (%s TEXT)' % (self.name, middle)

        return creator

    def selftablecreator(self):
        '''type in your own table-creator'''
        stmt = 'Type your SQL statement to create a %s.%s table from %s here: ' \
                % (self.credentials[3], self.name, self.csvfile)
        return raw_input(stmt)

    def csv2db(self):
        '''get csv data to database'''

        # using table already in database
        if not self.checktable():
            return 0

        # default commands
        destroyer = 'DROP TABLE IF EXISTS %s' % self.name
        loader = ("LOAD DATA LOCAL INFILE '%s' INTO TABLE %s FIELDS "
                  "TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' LINES "
                  "TERMINATED BY '\n' IGNORE 1 LINES" 
                  % (self.csvfile, self.name))

        # creating a table requires knowing the header
        question = ('Will you use the default table creator (0) '
                    'or supply your own (1)? ')
        ans = self.askthrice(question)
        if ans:
            creator = self.selftablecreator()
        else:
            creator = self.tablecreator()

        # execute commands
        # destroy old table, create new table, import new data into table
        # filter warning: http://stackoverflow.com/questions/4143686
        self.cur.execute(destroyer)
        # if bad default creator and ans = 1, then use default tablecreator()
        try: 
            self.cur.execute(creator)
        except _mysql_exceptions.ProgrammingError as e:
            if ans:
                print 'Your creator command failed. It was: '
                print '\t', creator
                print 'Now using default table creator.'
                try: 
                    self.cur.execute(self.tablecreator())
                except _mysql_exceptions.ProgrammingError as e:
                    print 'The default creator command failed. Sorry.'
                    sys.exit() # woo segfaults :p
            else:
                print 'The default creator command failed. Sorry.'
                print 'It was:'
                print '\t', repr(creator), '\n'
                sys.exit()
        # load csv file in.
        print 'Importing %s into the %s table.' \
               % (self.csvfile, self.credentials[3] + '.' + self.name)
        print 'This may take a while depending on how large your data is.'
        self.cur.execute(loader)

        # commit
        self.db.commit()
        self.cur.close()

        return 1

    def command(self):
        pass





