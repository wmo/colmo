#!/usr/bin/python

import json
import redis
import urllib

json_txt = urllib.urlopen("http://signpost/rome.json").read()
conf=json.loads(json_txt) 
r=redis.StrictRedis(host=conf["server"], port=conf["port"],db=0)

def main(): 
    n=r.delete("qjob") 
    n=r.delete("sprogress") 

if __name__ == "__main__": main()

