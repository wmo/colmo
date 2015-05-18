#!/usr/bin/python

# lf = list flags 

import json
import redis
import urllib

json_txt = urllib.urlopen("http://localhost/rome.json").read()
conf=json.loads(json_txt) 
r=redis.StrictRedis(host=conf["redisserver"], port=conf["redisport"],db=0)

def main(): 
    print "\n=== FLAGS ============== "
    for v in r.smembers("sflag"): print v

if __name__ == "__main__": main()



