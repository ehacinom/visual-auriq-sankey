#!/usr/local/bin/python

import csv, codecs, simplejson
from collections import defaultdict as dd

infile  = 'profiletest.csv'
outfile = 'profiletest.json'

# define dictionaries and lists to use
header = ["UserID", "convert", "timetoconvert", "duration_seen", 
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
days = { 0:'Sunday', 1:'Monday', 2:'Tuesday', 3:'Wednesday', 4:'Thursday', 
         5:'Friday', 6:'Saturday'}

# give default values for now before I figure out what went wrong
def whatbrowser(): return 'whatbrowser'
def whatOS(): return 'whatOS'
def whatday(): return 'whatday'

browsers = dd(whatbrowser, browsers)
operating = dd(whatOS, operating)
days = dd(whatday, days)

data   = open(infile, 'rU')
reader = csv.DictReader(data, delimiter=',', quotechar='"')

with codecs.open(outfile, 'w', encoding='utf-8') as out:
    for r in reader:
        for k, v in r.items():
            if not v:                                         # null generation
                r[k] = None
            elif k == 'browser' or k == 'browser_at_cv':      # browsers
                r[v] = operating[int(v)]
            elif k == 'OS' or k == 'os_at_cv':                # OS
                r[v] = browsers[int(v)]
            elif k == 'dayofweek_at_cv':                      # day of week
                r[v] = days[int(v)]
        out.write(simplejson.dumps(r, ensure_ascii=False) + '\n')