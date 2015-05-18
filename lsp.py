#!/usr/bin/python

# list the progress:
#    - the jobs in the job-queu
#    - the jobs being process ('in progress') 

import json
import redis
import urllib

json_txt = urllib.urlopen("http://localhost/rome.json").read()
conf=json.loads(json_txt) 
r=redis.StrictRedis(host=conf["redisserver"], port=conf["redisport"],db=0)

def main(): 
    print "\n=== PROGRESS ============== "
    n=r.llen("qjob") 
    print "--- QUEUE: qjob ------- : %d " % (n) 
    if (n<25):
        for i in range(0,n): 
            v=r.lindex("qjob",i)
            jobattrib=json.loads(v)
            print "%-10s %s %s" % ( jobattrib['id'], jobattrib['program'], jobattrib['variation'] )
    else:
        for i in range(0,10): 
            v=r.lindex("qjob",i)
            jobattrib=json.loads(v)
            print "%-10s %s %s" % ( jobattrib['id'], jobattrib['program'], jobattrib['variation'] )

        notshown=n-20
        #print "%s..%s not shown [ %d entries ]" % ( ja1['id'], ja2['id'], notshown ) 
        print "(not shown: %s..%s, %d entries)" % ( get_id_attrib(r.lindex("qjob",6)), get_id_attrib(r.lindex("qjob",n-6)),  (n-20) ) 
        for i in range(n-10,n): 
            v=r.lindex("qjob",i)
            jobattrib=json.loads(v)
            print "%-10s %s %s" % ( jobattrib['id'], jobattrib['program'], jobattrib['variation'] )
    

    print "--- SET: sprogress ------- "
    for v in r.smembers("sprogress"): 
        jobattrib=json.loads(v)
        print "%-10s %s %s" % ( jobattrib['id'], jobattrib['program'], jobattrib['variation'] )


def get_id_attrib(json_str): 
    attribs=json.loads(json_str)
    return attribs['id']


if __name__ == "__main__": main()

