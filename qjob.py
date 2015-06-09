#!/usr/bin/python

import argparse
import json
import os.path
import re
import redis
import sys 
import time
import urllib

json_txt = urllib.urlopen("http://localhost/rome.json").read()
conf=json.loads(json_txt) 
r=redis.StrictRedis(host=conf["redisserver"], port=conf["redisport"],db=0)

def main(): 
    # cmd line args
    parser = argparse.ArgumentParser(description='Put jobs on the Colmo queue.')

    parser.add_argument('-dry', action='count', help='dry run, just show what would be done, don''t execute')
    parser.add_argument('-ioe', nargs='+', action='store', help='input files, one for each script') 
    parser.add_argument('-iae', nargs='+', action='store', help='input files, all for each script') 
    parser.add_argument('-e', nargs='+', action='store', help='executable script')
    parser.add_argument('-e2', nargs='+', action='store', help='2nd executable script')
    parser.add_argument('-ow', nargs='+', action='store', help='output files, wait for output') 
    parser.add_argument('-o', nargs=1, action='store', help='output files, but don''t wait for output') 
    parser.add_argument('-oc', nargs=1, action='store', help='output files to be concatenated') 
    parser.add_argument('-och', nargs=1, action='store', help='output files to be concatenated, skip header on file 2,3,...') 
    parser.add_argument('-pf',  action='count', help='pipe output file as input to the next script') 
    parser.add_argument('-pi', action='count', help='pipe job-id only to the next script') 
    parser.add_argument('args', nargs=argparse.REMAINDER)

    args= parser.parse_args() 
    print(args)

        
    #    if len(sys.argv)<4:
    #print "Usage: %s [inputfile1] [inputfile2] [..] [-o outputfile1 [outputfile2] [..]] prog var1 [var2] [var3] .." % sys.argv[0]
    #    sys.exit(2)

    # pattern: multiple datafiles + program file + multiple variations
    # the program file ends with either ".R", ".m", or ".py" (that's the way it's recognized as a program

    # step 1: the program file, at least one
    if args.e==None or len(args.e)<1: 
        print "Need at least 1 program file to execute (an R or python script). eg. '-e YOURSCRIPT.R'"
        sys.exit(2)
    program_name=args.e[0]
    print "program -------- %s " % (program_name)
    if not os.path.exists( program_name): 
        print "Program %s not found!" % program_name 
        sys.exit(2)

    if args.dry==None:
        add_to_tagmap("mprogram", program_name, True )  # Force put program source onto redis 

    # the input files
    infile_ls=[] 
    if args.ioe: 
        infile_ls=args.ioe
    elif args.iae: 
        infile_ls=args.iae
    # put them on redis (if not yet there)
    for fn in infile_ls:
        print "inputfile ------ %s " % (fn)
        if args.dry==None:
            add_to_tagmap("mdata", fn, False) # put data onto redis, but don't force it

    # the output files
    wait_for_output=False # should this program wait for the output?
    concat_output=False   # should the output automatically be concatenatned?
    concat_skip_header=False   # when concatenating, skip 1 headerline for file 2,3,.. 
    if args.ow or args.oc or args.och: 
        wait_for_output=True # yes this program should wait for the output
    outfile_ls=[]
    if args.ow:     # output files follow, wait for the output
        outfile_ls=args.ow
        wait_for_output=True
    elif args.o:    # output files follow, but don't wait for output
        outfile_ls=args.o
    elif args.oc:   # output file(s) follow, concatenate
        outfile_ls=args.oc
        wait_for_output=True
        concat_output=True
    elif args.och:   # output file(s) follow, concatenate, skipping header on file 2,3,..
        outfile_ls=args.och
        wait_for_output=True
        concat_output=True
        concat_skip_header=True

    for fn in outfile_ls:
        print "outputfile ----- %s " % (fn)
        #if args.dry==None:

    # expand the variations
    variation_ls=[] 
    if args.e:
        for var in args.e[1:]:
            variation_ls.extend(expand_variation(var)) # extend = append vector to vector

    # the jobs
    jobs_added_ls=[]
    if args.ioe:    # one input file for each script, variations are ignored
        for infile in infile_ls: 
            print "infile    ------ %s" % infile
            jobs_added_ls.append( add_job(program_name,[infile],outfile_ls, '') ) # infile name is going to be passed as arg
    elif args.iae:  # all input files for each script         
        for var in variation_ls:
            print "variation ------ %s" % var
            jobs_added_ls.append( add_job(program_name,infile_ls,outfile_ls, var) ) # infile name is going to be passed as arg

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
