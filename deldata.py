#!/usr/bin/python

import json
import redis
import sys
import urllib

json_txt = urllib.urlopen("http://localhost/rome.json").read()
conf=json.loads(json_txt) 
r=redis.StrictRedis(host=conf["redisserver"], port=conf["redisport"],db=0)

def main(): 
    # del data
    if len(sys.argv)<1:
        sys.exit("List the files you want to delete.")

    for fn in sys.argv[1:]:
        r.hdel("mdata",fn)


if __name__ == "__main__": main()

