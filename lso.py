#!/usr/bin/python

import json
import redis
import urllib

json_txt = urllib.urlopen("http://localhost/rome.json").read()
conf=json.loads(json_txt) 
r=redis.StrictRedis(host=conf["redisserver"], port=conf["redisport"],db=0)

def main(): 
    print "\n=== OUTPUTS ============== "
    print "--- SET: sdone ------- "
    for v in r.smembers("sdone"): print v

    print "--- MAP: moutput ------- "
    for v in r.hkeys("moutput"): print v

if __name__ == "__main__": main()

