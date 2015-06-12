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
    json_txt=r'{"command":"short_sleep"}'
    r.sadd('sflag', json_txt) 
    print "Flag: %s " % json_txt

if __name__ == "__main__": main()
