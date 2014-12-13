#!/usr/bin/python

import json
import redis
import urllib

json_txt = urllib.urlopen("http://signpost/rome.json").read()
conf=json.loads(json_txt) 
r=redis.StrictRedis(host=conf["server"], port=conf["port"],db=0)

def main(): 
    for attrib in [ 'mprogram', 'mdata', 'mvariation' ]: 
        print "--- %s ------- " % attrib
        for v in r.hkeys(attrib): 
            print "Deleting " + v
            r.hdel(attrib,v)

if __name__ == "__main__": main()


