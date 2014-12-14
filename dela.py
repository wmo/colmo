#!/usr/bin/python

import json
import redis
import urllib

json_txt = urllib.urlopen("http://localhost/rome.json").read()
conf=json.loads(json_txt) 
r=redis.StrictRedis(host=conf["server"], port=conf["port"],db=0)

def main(): 
    # del inputs
    for attrib in [ 'mprogram', 'mdata', 'mvariation' ]: 
        #print "--- %s ------- " % attrib
        for v in r.hkeys(attrib): 
            r.hdel(attrib,v)
            #print "Deleting " + v

    # del progress items    
    n=r.delete("qjob") 
    n=r.delete("sprogress") 

    # del outputs
    r.delete("sdone")
    for v in r.hkeys("moutput"): 
        # print "Deleting " + v
        r.hdel("moutput",v)


if __name__ == "__main__": main()

