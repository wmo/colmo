#!/usr/bin/python

import json
import redis
import re
import urllib

json_txt = urllib.urlopen("http://localhost/rome.json").read()
conf=json.loads(json_txt) 
r=redis.StrictRedis(host=conf["redisserver"], port=conf["redisport"],db=0)

def main(): 
    # first setup a dictionary with the execution time of each job
    jobtime=dict()
    for m in sorted(r.smembers("sdone")): 
        data=json.loads(m)
        dtime=float(data["exec_end"])-float(data["exec_start"])
        jobtime[data["id"]]=round(dtime,2)

    print "--- MAP: moutput ------- "
    for k in sorted(r.hkeys("moutput")): 
        data=json.loads(r.hget("moutput",k))
        #print  data 
        v=data["output"]
        o=""
        if isinstance(v,list):
            o=re.sub('\| +\|','||',   re.sub( '\n',' | ', ' | '.join(v))  )
        else:
            o=v
        t=0
        if k in jobtime:
            t=jobtime[k]
        print "%7.2fs %s: %s" % ( t,k,o )

if __name__ == "__main__": main()

