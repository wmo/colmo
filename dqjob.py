#!/usr/bin/python

# remove a number of jobs from the front of the queue and discard
# dq = de-queue

import redis
import json
import time
import sys 
import urllib
import os.path

json_txt = urllib.urlopen("http://localhost/rome.json").read()
conf=json.loads(json_txt) 
r=redis.StrictRedis(host=conf["redisserver"], port=conf["redisport"],db=0)

def main(): 
    # cmd line args
    if len(sys.argv)<2:
        print "Usage: %s n " % sys.argv[0]
        print "       (this will 'pop' the next 'n' jobs from the queue)" 
        sys.exit(2)

    numjobs_to_pop= int(sys.argv[1]) 
    for i in range(1,numjobs_to_pop):
        json_txt=r.rpop( "qjob" ) 
        jobattrib=json.loads(json_txt)
        print "Removed: %-10s %s %s" % ( jobattrib['id'], jobattrib['program'], jobattrib['variation'] )


if __name__ == "__main__": main()
