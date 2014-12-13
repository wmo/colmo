#!/usr/bin/python

import json
import redis
import re
import urllib

json_txt = urllib.urlopen("http://signpost/rome.json").read()
conf=json.loads(json_txt) 
r=redis.StrictRedis(host=conf["server"], port=conf["port"],db=0)

def main(): 
    print "--- MAP: moutput ------- "
    for k in sorted(r.hkeys("moutput")): 
        data=json.loads(r.hget("moutput",k))
        #print  data 
        v=data["output"]
        o=""
        if isinstance(v,list):
            o=re.sub( '\n',' | ', ' | '.join(v) ) 
        else:
            o=v
        print "%s : %s" % ( k, o )

if __name__ == "__main__": main()

