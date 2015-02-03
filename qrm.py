#!/usr/bin/python

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
    # cmd line args: a number, how many nodes are to be cleaned
    if len(sys.argv)<2:
        print "Need a numeric argument !\n" 
        print "Usage: %s 4 \n (for cleaning up 4 morsels)" % sys.argv[0]
        sys.exit(2)

    # get a job_id
    sq=r.incr("seq_job")
    tm=r.time()  # unix timestamp + microseconds

    for i in range(0,int(sys.argv[1])): 
        tm=r.time()  # unix timestamp + microseconds
        json_txt='{' + str.format( r'"id":"job{0}:{1}","command":"cleanup" , "queue_time":"{2}.{3}"', 
                                       sq, i, tm[0],tm[1] ) +'}' 
        r.lpush( "qjob", json_txt) 
        print "Adding %s" % json_txt 

if __name__ == "__main__": main()
