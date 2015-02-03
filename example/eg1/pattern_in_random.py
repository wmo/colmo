#!/usr/bin/python 

import sys
import random 

if len(sys.argv)<2:
    print "Usage: %s pattern " % sys.argv[0]
    print "   eg. %s 7777 " % sys.argv[0]
    sys.exit(1)

pattern=sys.argv[1]
cnt=0
random.seed()
go_on=True
while go_on:
    s=str.format("{0}",random.randrange(0,100000000))
    if s.endswith(pattern): 
        print "| Python | pattern-length:%d | iterations:%d | found:%s |" % ( len(pattern), cnt,s ) 
        go_on=False
    cnt=cnt+1
