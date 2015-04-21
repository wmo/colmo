#!/usr/bin/python 

import sys
import random 

if len(sys.argv)<2:
    print "Usage: %s echo_arg " % sys.argv[0]
    print "   eg. %s 7777 " % sys.argv[0]
    sys.exit(1)

print "%s | %s " % ( sys.argv[0], sys.argv[1:] ) 
