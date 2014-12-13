#!/usr/bin/python

import json
import redis
import urllib

json_txt = urllib.urlopen("http://signpost/rome.json").read()
conf=json.loads(json_txt) 
r=redis.StrictRedis(host=conf["server"], port=conf["port"],db=0)

def main(): 
    print "deleting sdone" 
    r.delete("sdone")

    print "deleting moutput" 
    for v in r.hkeys("moutput"): 
        print "Deleting " + v
        r.hdel("moutput",v)

if __name__ == "__main__": main()

