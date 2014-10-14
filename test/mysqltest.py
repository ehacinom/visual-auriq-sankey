import sys
import sqlite3
import MySQLdb
import re

def get_rules(credentials, i):
    """Grab relevant user-created rules from the MySQL database 
    on essclone.auriq.com
    
    :param: credentials - an iterable (host, user, passwd, db) options for MySQLdb.connect()
    :param: i - rule_owner in question by integer index
    :return: 
    """
    (MYSQL_HOST, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DB) = credentials

    db = MySQLdb.connect(host=MYSQL_HOST, passwd=MYSQL_PASSWORD, user=MYSQL_USER, db=MYSQL_DB)
    cur = db.cursor()
    stmt = ('SELECT id, rule, rule_type, rule_owner, rule_name FROM '
            'category_rule WHERE rule_owner = 0 OR rule_owner = %s')
    cur.execute(stmt, (i,))

    rules = cur.fetchall()

    # print all the first cell of all the rows
    print 'THESE ARE THE RULES'
    for row in rules:
        print '\t', row
    print 'END RULES'
    
    return rules

def regexp(expr, item):
    """Define REGEXP function for SQLite3 connection

    :param: expr - regex
    :item: string to match
    :return: 1,0 whether there is an item match to the expr
    """
    reg = re.compile(expr)
    x = reg.search(item)
    return x is not None

def set_rules(credentials, i):
    """Apply user-created rules to SQLite database in .conf/

    :param: credentials - an iterable (host, user, passwd, db) options for MySQLdb.connect()
    :param: i - rule_owner in question by integer index
    """

    with sqlite3.connect('test.db') as con:
        con.create_function('REGEXP', 2, regexp)
        cur = con.cursor()

        for _, rule, kind, _, name in get_rules(credentials, i):
            # don't let empty count and match all
            if not rule:
                continue

            rule = '(' + rule + ')'
            if kind: # filenames
                stmt = 'SELECT filename FROM fileindex WHERE filename REGEXP ?'
                cur.execute(stmt, [rule])
                files = cur.fetchall()
                stmt = 'UPDATE fileindex SET categoryname = ? WHERE '
            
            
            print len(x), rule, kind, name

if __name__ == "__main__":
    credentials = ('localhost', 'root', 'kahasi', 'mydmp')
    set_rules(credentials, 8)
