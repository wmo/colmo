#!/usr/bin/python

import json
import redis
import urllib

json_txt = urllib.urlopen("http://signpost/rome.json").read()
conf=json.loads(json_txt) 
r=redis.StrictRedis(host=conf["server"], port=conf["port"],db=0)

def main(): 
    print "\n=== PROGRESS ============== "
    print "--- QUEUE: qjob ------- "
    n=r.llen("qjob") 
    for i in range(0,n): 
        print r.lindex("qjob",i)

    print "--- SET: sprogress ------- "
    for v in r.smembers("sprogress"): print v


if __name__ == "__main__": main()

