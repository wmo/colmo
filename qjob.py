#!/usr/bin/python

import redis
import json
import time
import sys 
import urllib
import os.path
import re

json_txt = urllib.urlopen("http://localhost/rome.json").read()
conf=json.loads(json_txt) 
r=redis.StrictRedis(host=conf["redisserver"], port=conf["redisport"],db=0)

def main(): 
    # cmd line args
    if len(sys.argv)<4:
        print "Usage: %s [inputfile1] [inputfile2] [..] [-o outputfile1 [outputfile2] [..]] prog var1 [var2] [var3] .." % sys.argv[0]
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
    wait_for_output=False # should this program wait for the output?
    datafile_ls=sys.argv[1:prog_index]
    infile_ls=[]
    outfile_ls=[]
    is_input=True # if false, it is an output
    for dfn in datafile_ls: 
        if dfn=="-wo":       # wait for the output, and show output
            wait_for_output=True
            continue 
        if dfn=="-o":       # outputfile name is following, wait for it
            is_input=False 
            wait_for_output=True
            continue 
        if dfn=="-O":       # outputfile name is following, but DON'T wait for it
            is_input=False 
            wait_for_output=False
            continue 
        if is_input:
            print "inputfile ------ %s " % (dfn)
            add_to_tagmap("mdata", dfn, False) # Don't force-put data onto redis
            infile_ls.append(dfn)
        else:
            print "outputfile ----- %s " % (dfn)
            outfile_ls.append(dfn)

    # step 3: the variations
    variation_ls=[] 
    for var in sys.argv[prog_index+1:]:
        variation_ls.extend(expand_variation(var)) # extend = append vector to vector

    if len(variation_ls)<1:
        variation_ls.append("1") 

    jobs_added_ls=[]
    for var in variation_ls:
        print "variation ------ %s" % var
        jobs_added_ls.append( add_job(program_name,infile_ls,outfile_ls, var) )

    l=len(jobs_added_ls)
    if l<25:
        print "Added jobs: {}".format(jobs_added_ls)
    else:
        print "Added jobs: {} {} .. {} {} ".format(jobs_added_ls[0],jobs_added_ls[1],
            jobs_added_ls[l-2] ,jobs_added_ls[l-1])

    # step 4: poll for the output, in case we have output files
    if len(outfile_ls)>0 and wait_for_output: 
        print "Waiting for output files: {} ".format(",".join(outfile_ls))
        job_waited_on_set=set(jobs_added_ls)
        report_set_len=True
        while (len(job_waited_on_set)>0):
            if report_set_len:
                sys.stdout.write( "\n["+str(len(job_waited_on_set))+"]" )
                report_set_len=False
            #for job in job_waited_on_set:
            for job_json in r.smembers("sdone"): 
                jobattrib=json.loads(job_json)
                jobid=jobattrib['id'] 
                if jobid in job_waited_on_set:
                    # job is done, get the output file(s)
                    for ofn in outfile_ls:
                        fn="{}-{}".format( jobid, ofn) 
                        content=r.hget("mdata",fn)
                        write_file(fn,content)
                    # remove from set
                    job_waited_on_set.remove(jobid)
                    report_set_len=True
            sys.stdout.write(".") ; sys.stdout.flush()
            time.sleep(1) # sleep a second
    elif len(outfile_ls)==0 and wait_for_output:    # wait for completion and show output
        print ""
        job_waited_on_set=set(jobs_added_ls)
        report_set_len=True
        while (len(job_waited_on_set)>0):
            for job_json in r.smembers("sdone"): 
                jobattrib=json.loads(job_json)
                jobid=jobattrib['id'] 
                if jobid in job_waited_on_set:
                    data=json.loads(r.hget("moutput",jobid))
                    v=data["output"]
                    o=""
                    if isinstance(v,list):
                        o=re.sub( '\n',' | ', ' | '.join(v) )
                    else:
                        o=v
                    print "[%3d] %s: %s" % ( len(job_waited_on_set),jobid, o )

                    # remove from set
                    job_waited_on_set.remove(jobid)
            #sys.stdout.write(".") ; sys.stdout.flush()
            time.sleep(1) # sleep a second
    print "\nDone"



def add_job(program_name, infile_ls, outfile_ls, variation_name): 
        # get a job_id
        sq=r.incr("seq_job")
        tm=r.time()  # unix timestamp + microseconds
        jobid="j{}".format(sq)
        # push on the qjob queue
        r.lpush( "qjob", '{' + str.format( 
            r'"id":"{0}","program":"{1}", "infile_ls":{2}, "outfile_ls":{3}, "variation":"{4}", "queue_time":"{5}.{6}"', 

            jobid, program_name, json.dumps(infile_ls), json.dumps(outfile_ls), variation_name, tm[0],tm[1] ) +'}' )
        return jobid


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
    if "~" in variation: 
        if variation.count("~")==1: 
            (r,c)=variation.split("~") 
            row_ls=expand_range_or_list(r) 
            col_ls=expand_range_or_list(c) 
            #print "ROW {}".format(row_ls)
            #print "COL {}".format(col_ls)
            for re in row_ls:
                for ce in col_ls:
                    rv_ls.append( "{} {}".format(re,ce) ) 
        elif variation.count("~")==2: 
            (r,c,d)=variation.split("~") 
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
    if "~" in s: 
        sys.exit("No '~' allowed in range or list")
    if ":" in s:
        (start,stop,step)=range_details(s)
        while start<=stop:
            rv_ls.append(start)
            start+=step
    elif "," in s:
        rv_ls.extend( s.split(",") ) 
    else:
        rv_ls.append( s )
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



def write_file(filename,content):
    if content==None: 
        print "Warning: content==None %s" % filename
        return
    if content and (len(content)<1): 
        print "Warning: no content for %s" % filename
        return
    sys.stdout.write(" ") ; sys.stdout.write(filename) ; sys.stdout.write(" ") 
    fw = open(filename,"wb")
    fw.write(content)
    fw.close()


if __name__ == "__main__": main()
