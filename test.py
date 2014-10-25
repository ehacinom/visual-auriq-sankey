#!/usr/bin/python

from csv2json import JSONfun

credentials = ('localhost', 'root', 'kahasi', 'mydmp')
name = 'profiletest'

data = JSONfun(credentials, name)


CREATE TABLE profiletest (UserID TEXT, convert TEXT, timetoconvert TEXT, duration_seen TEXT, events_total TEXT, events_before_cv TEXT, browser TEXT, fractionBrowser TEXT, OS TEXT, fractionOS TEXT, browser_at_cv TEXT, os_at_cv TEXT, dayofweek_at_cv TEXT, timeofday_at_cv TEXT)
