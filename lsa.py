#!/usr/bin/python

import json
import redis
import urllib

json_txt = urllib.urlopen("http://signpost/rome.json").read()
conf=json.loads(json_txt) 
r=redis.StrictRedis(host=conf["server"], port=conf["port"],db=0)

def main(): 
    print "\n=== INPUTS ============== "
    print "--- MAP: mprogram ------- "
    for v in r.hkeys("mprogram"): print v

    print "--- MAP: mdata ------- "
    for v in r.hkeys("mdata"): print v

    print "--- MAP: mvariation ------- "
    for v in r.hkeys("mvariation"): print v

    print "\n=== PROGRESS ============== "
    print "--- QUEUE: qjob ------- "
    n=r.llen("qjob") 
    for i in range(0,n): 
        print r.lindex("qjob",i)

    print "--- SET: sprogress ------- "
    for v in r.smembers("sprogress"): print v

    print "\n=== OUTPUTS ============== "
    print "--- SET: sdone ------- "
    for v in r.smembers("sdone"): print v

    print "--- MAP: moutput ------- "
    for v in r.hkeys("moutput"): print v

if __name__ == "__main__": main()

