#!/usr/bin/python

# lower the flag for suspending a host
# (lf=lower-flag)

import redis
import json
import time
import sys 
import urllib
import os.path

json_txt = urllib.urlopen("http://localhost/rome.json").read()
conf=json.loads(json_txt) 
r=redis.StrictRedis(host=conf["redisserver"], port=conf["redisport"],db=0)

def main(): 
    # cmd line args
    if len(sys.argv)<2:
        print "Usage: %s hostname " % sys.argv[0]
        print "       (this will 'lower' the suspend flag for 'hostname')" 
        sys.exit(2)

    for v in r.smembers("sflag"):
        flagattrib=json.loads(v)
        if flagattrib["command"]=="suspend_host":
            if flagattrib["hostname"]== sys.argv[1]:
                print "Lowered flag : %s " % v
                r.srem('sflag', v ) # remove it


if __name__ == "__main__": main()
