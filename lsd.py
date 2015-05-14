#!/usr/bin/python

import json
import redis
import urllib

json_txt = urllib.urlopen("http://localhost/rome.json").read()
conf=json.loads(json_txt) 
r=redis.StrictRedis(host=conf["redisserver"], port=conf["redisport"],db=0)

def main(): 
    print "\n=== DATA ============== "
    print "--- MAP: mdata ------- "
    for v in r.hkeys("mdata"): print v

if __name__ == "__main__": main()

