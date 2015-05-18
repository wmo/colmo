#!/usr/bin/python

# raise the flag for suspending a host
# (fr=flag_raise)

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
        print "       (this will 'raise' the suspend flag for 'hostname'," 
        print "        for graceful shutdown of the morsels on a host)" 
        sys.exit(2)

    json_txt='{' + str.format( r'"command":"suspend_host", "hostname":"{}"', sys.argv[1]) + '}'
    r.sadd('sflag', json_txt) 
    print "Flag: %s " % json_txt


if __name__ == "__main__": main()
