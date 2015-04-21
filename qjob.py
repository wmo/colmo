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
    # cmd line args
    if len(sys.argv)<4:
        print "Usage: %s [datafile1] [datafile2] .. prog var1 [var2] [var3] .." % sys.argv[0]
        sys.exit(2)

    # pattern: multiple datafiles + program file + multiple variations
    # the program file ends with either ".R", ".m", or ".py" (that's the way it's recognized as a program

    # step 1: find the program file
    prog_index=-1
    for i in range(1,len(sys.argv) ) : 
        if sys.argv[i].endswith(".py") or sys.argv[i].endswith(".R") or sys.argv[i].endswith(".m"): 
                prog_index=i
                program_name=sys.argv[i]
                break

    if prog_index==-1:
        sys.exit("No program found in the argument list. Acceptable programs are: .py, .R, .m") 
    
    print "program -------- %s " % (program_name)
    add_to_tagmap("mprogram", program_name, True )  # Force put program onto redis 


    # step 2: the data argument slice is that from 1..prog_index
    datafile_ls=sys.argv[1:prog_index]
    for dfn in datafile_ls: 
        print "datafile ------- %s " % (dfn)
        add_to_tagmap("mdata", dfn, False) # Don't force-put program onto redis

    # step 3: the variations
    variation_ls=[] 
    for var in sys.argv[prog_index+1:]:
        variation_ls.extend(expand_variation(var)) # extend = append vector to vector

    for var in variation_ls:
        print "variation ------ %s" % var
        add_job(program_name,",".join(datafile_ls), var) 


def add_job(program_name,data_names, variation_name): 
        #add_to_tagmap("mvariation", variation_name, true)

        # get a job_id
        sq=r.incr("seq_job")
        tm=r.time()  # unix timestamp + microseconds
        r.lpush( "qjob", '{' + str.format( 
            r'"id":"job{0}","program":"{1}", "data":"{2}", "variation":"{3}", "queue_time":"{4}.{5}"', 
                 sq, program_name,data_names, variation_name, tm[0],tm[1] ) +'}' )
        print "Adding  job%s" % sq


def add_to_tagmap(map_name,field,force):
    # first check if the tag already exists, unless we are forced to put it onto redis
    if (not force) and (r.hexists(map_name,field)):
        print "  map '%s' already contains '%s' " % ( map_name, field ) 
        return

    # if it is a file, put it on the queue
    if os.path.isfile(field):
        # read the file represented by 'field'
        with open(field, 'r') as content_file:
            content = content_file.read()
            r.hset(map_name,field,content)


def expand_variation(variation): 
    rv_ls=[]
    #print "VAR=%s" % variation
    if "x" in variation: 
        if variation.count("x")==1: 
            (r,c)=variation.split("x") 
            row_ls=expand_range_or_list(r) 
            col_ls=expand_range_or_list(c) 
            for re in row_ls:
                for ce in col_ls:
                    rv_ls.append( "{} {}".format(re,ce) ) 
        elif variation.count("x")==2: 
            (r,c,d)=variation.split("x") 
            row_ls=expand_range_or_list(r) 
            col_ls=expand_range_or_list(c) 
            dep_ls=expand_range_or_list(d) 
            for re in row_ls:
                for ce in col_ls:
                    for de in dep_ls:
                      rv_ls.append( "{} {} {}".format(re,ce,de))
        else:
            sys.exit("{} : maximum number of dimensions is 3 eg. 3x5x2.".format(variation)) 
    elif ":" in variation or "," in variation:
        rv_ls.extend( expand_range_or_list(variation) ) 
    else:
        rv_ls.append(variation)
    return rv_ls


# s may contain : or , (but not x)
def expand_range_or_list(s): 
    rv_ls=[]
    if "x" in s: 
        sys.exit("No 'x' allowed in range or list")
    if ":" in s:
        (start,stop,step)=range_details(s)
        while start<stop:
            rv_ls.append(start)
            start+=step
    elif "," in s:
        rv_ls.extend( s.split(",") ) 
    return rv_ls



def range_details(s): 
    step="1"
    start="1"
    stop="1"
    if s.count(":")==1: 
        (start,stop)=s.split(":") 
    elif s.count(":")==2: 
        (start,stop,step)=s.split(":") 
    else:
        sys.exit("{} : Maximum number of dimensions is 3 eg. 3:5:0.25".format(s)) 
    if "." in start or "." in stop or "." in step: 
        return float(start), float(stop), float(step)
    return int(start), int(stop), int(step)




if __name__ == "__main__": main()
