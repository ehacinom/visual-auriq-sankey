#!/usr/bin/python

from csv2json import JSONfun

credentials = ('localhost', 'root', 'kahasi', 'mydmp')
name = 'profiletest'

data = JSONfun(credentials, name)
