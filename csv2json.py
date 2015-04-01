import MySQLdb  # Server version: 5.5.40 MySQL Community Server (GPL)
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
    # localhost, root, password, mydmp 
    name = '$NAME'  
    data = JSONfun(credentials, name)
    data.tosankey()
      
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

        # outfile
        self.jsonfile = self.name + '.json'

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
            # backticks for reserved words, see below
            self.cols = ['`'+c+'`' for c in header]
        else:
            self.cols = ['C'+str(i) for i in xrange(self.n)]

        # I hate the problem of reserved words
        # mysql 5.5.40 for me
        # should use sets to check for matches and remove, but I'm just
        # going to assume backticks and intelligent mysql database users

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
            print 'You used creator command: \n\t', creator
        except _mysql_exceptions.ProgrammingError as e:
            defaultfail = ('The default creator command failed. Sorry.\n'
                           'Check the csv2json.py file or your command for '
                           'column names that are RESERVED WORDS for the '
                           'MySQL server.\nThe command used was:\n\t%s' 
                           % creator)
            if ans:
                print 'Your creator command failed. It was:\n\t', creator
                print 'Probably due to: reserved words w/out backticks.'
                print 'Now using default table creator.'
                try: 
                    self.cur.execute(self.tablecreator())
                except _mysql_exceptions.ProgrammingError as e:
                    print e, defaultfail
                    sys.exit() # woo segfaults :p
            else:
                print e, defaultfail
                sys.exit()
        # load csv file in.
        print 'Importing %s into the %s table.' \
               % (self.csvfile, self.credentials[3] + '.' + self.name)
        print 'This may take a while depending on how large your data is.'
        self.cur.execute(loader)

        # commit
        self.db.commit()
        # self.cur.close() # don't close!

        return 1

    def countcmd(self, *args):
        '''Return the int count value.
        Create the command by adding together strings.
        '''
        cmd = 'SELECT COUNT(*) FROM %s' % self.name
        n = len(args)

        # this makes me unhappy
        if n/2: cmd += ' WHERE '
        for i in xrange(n):
            if i % 2: continue
            if i != 0: cmd += 'and '
            cmd += '%s = %s '% (args[i], args[i+1])

        self.cur.execute(cmd)
        return self.cur.fetchone()[0]

    def profiletosankey(self, b):
        '''the multiprocessed function call for work14
        :param: b - browser we are dealing with
        
        This is so slow.
        '''

        # browser to nonconversion
        self.nonconversion[self.browsers[b]+self.s, self.ncv[0]] += self.countcmd(self.cv[2], self.ncv[1], 'browser', b)
        # browser to conversion
        self.conversion[self.browsers[b], self.cv[0]] += self.countcmd(self.cv[2], self.cv[1], 'browser', b)

        # os to browser; differentiating by ncv/cv slows it down by 3 times
        for os in self.operating:
            # os to browser to nonconversion
            self.nonconversion[self.operating[os]+self.s+self.browsers[b][0], self.browsers[b]+self.s] += self.countcmd('OS', os, 'browser', b, self.cv[2], self.ncv[1])
            # os to browser to conversion
            self.conversion[self.operating[os]+self.browsers[b][0], self.browsers[b]] += self.countcmd('OS', os, 'browser', b, self.cv[2], self.cv[1])

    def nodemaker(self, links, extra=[]):
        '''Convert integer 2-keyed links dictionary to nodes list.
        Optional extra/single parameter to add to the node list'''
        nodes = [s for (s,t), v in links.items() if v > 0]
        nodes.append(extra)
        nodes = set(nodes)
        return [{"name": n} for n in nodes]

    def linkmaker(self, links):
        '''Convert integer 2-keyed links dictionary to links list'''
        return [{"source":s, "target":t, "value":v} for (s,t), v in links.items() if v > 0]

    def tosankey(self):
        '''db to sankey json format.
        needs to be multiprocessed'''

        # define needed constants
        self.constants()

        # :( mp will thro a PicklingError
        # PicklingError: Can't pickle <type 'instancemethod'>: attribute lookup __builtin__.instancemethod failed
        # pool = mp.Pool(processes=4)
        # pool.apply_async(self.profiletosankey, self.browsers)
        # pool.close()
        # pool.join()

        print 'Counting needed values with database commands. Please wait.'
        for b in frogress.bar(self.browsers):
            self.profiletosankey(b)

        # write to the dictionary and json
        print '\nWriting nodes to json.'
        nodesncv = self.nodemaker(self.nonconversion, self.ncv[0])
        nodescv  = self.nodemaker(self.conversion, self.cv[0])
        self.nodes = nodesncv + nodescv

        print 'Writing links to json.'
        linksncv = self.linkmaker(self.nonconversion)
        linkscv  = self.linkmaker(self.conversion)
        self.links = linksncv + linkscv
        
        print 'The final file is at %s' % self.jsonfile
        def writejson(links, nodes, outfile):
            '''lol so much repeating and helper functions'''
            linksandnodes = {"links":links, "nodes":nodes}
            with open(outfile, 'w') as f:
                json.dump(linksandnodes, f)

        writejson(self.links, self.nodes, self.jsonfile)

    def constants(self):
        '''defining the things we need for this particular implmentation, work14
        should be automated but is not
        '''

        # these next two are from the profile we got from colin a few months ago
        browsers = { 1:'IE', 2:'IE', 8:'IE', 9:'FF', 10:'SAFARI', 11:'IE', 12:'IE', 
                     13:'CHROME', 14:'FF', 15:'FF', 16:'SAFARI', 17:'SAFARI', 
                     18:'SAFARI', 19:'SAFARI', 20:'IE', 21:'FF', 25:'IE', 
                     26:'SAFARI', 27:'SAFARI', 28:'SAFARI', 29:'IE', 30:'IE', 
                     5:'NN', 22:'OPERA', 24:'OPERA' }
        operating = { 0:'Unknown', 1:'WINDOWS', 2:'WINDOWS', 3:'WINDOWS', 4:'WINDOWS', 
                      5:'WINDOWS', 6:'WINDOWS', 8:'WINDOWS', 9:'WINDOWS', 10:'iOS', 
                      11:'iOS', 12:'iOS', 13:'android', 14:'android', 17:'android', 
                      18:'android', 20:'android', 21:'android', 22:'WINDOWS', 
                      7:'MAC', 23:'MAC',  19:'windows phone'}
        self.browsers = browsers
        self.operating = operating
        
        # [word, str(integer)]
        self.cv  = ['conversion', '1', '`convert`']
        self.ncv = ['nonconversion', '0']

        # default storage
        self.conversion = defaultdict(int)
        self.nonconversion = defaultdict(int)

        # distinguishing space
        self.s = ' '

    def command(self):
        pass





