#!/usr/bin/python

import redis
import json
import time
import sys 
import urllib
import os.path

json_txt = urllib.urlopen("http://signpost/rome.json").read()
conf=json.loads(json_txt) 
r=redis.StrictRedis(host=conf["server"], port=conf["port"],db=0)

def main(): 
    # cmd line args
    if len(sys.argv)<4:
        print "Usage: %s prog data var [var2] [var3] .." % sys.argv[0]
        sys.exit(2)

    program_name=sys.argv[1]
    add_to_tagmap("mprogram", program_name)

    data_name=sys.argv[2]
    add_to_tagmap("mdata", sys.argv[2])

    for i in range(3,len(sys.argv)):
        variation_name=sys.argv[i]
        add_job(program_name,data_name, variation_name) 


def add_job(program_name,data_name, variation_name): 
        add_to_tagmap("mvariation", variation_name)

        # get a job_id
        sq=r.incr("seq_job")
        tm=r.time()  # unix timestamp + microseconds
        r.lpush( "qjob", '{' + str.format( 
            r'"id":"job{0}","program":"{1}", "data":"{2}", "variation":"{3}", "queue_time":"{4}.{5}"', 
                 sq, program_name,data_name, variation_name, tm[0],tm[1] ) +'}' )
        print "Adding  job%s" % sq


def add_to_tagmap(map_name,field):
    # first check if the tag already exists 
    if r.hexists(map_name,field):
        print "  map '%s' already contains '%s' " % ( map_name, field ) 
        return

    # if it is a file, put it on the queue
    if os.path.isfile(field):
        # read the file represented by 'field'
        with open(field, 'r') as content_file:
            content = content_file.read()
            r.hset(map_name,field,content)

if __name__ == "__main__": main()
