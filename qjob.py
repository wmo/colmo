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

    parser.add_argument('-ci', action='count', help='convey input file names as parameters to the script') 
    parser.add_argument('-co', action='count', help='convey output file names as parameters to the script') 
    parser.add_argument('-ca', action='count', help='convey both input and output file names as parameters to the script') 
    parser.add_argument('-cn', action='count', help='convey no files names as parameters to the script') 
    parser.add_argument('-dry', action='count', help='dry run, just show what would be done, don''t execute') #NOT OK YET
    parser.add_argument('-ioe', nargs='+', action='store', help='input files, one for each script') 
    parser.add_argument('-iae', nargs='+', action='store', help='input files, all for each script') 
    parser.add_argument('-x', nargs='+', action='store', help='executable script')
    parser.add_argument('-x2', nargs='+', action='store', help='2nd executable script')
    parser.add_argument('-ow', nargs='+', action='store', help='output files, wait for output') 
    parser.add_argument('-o', nargs=1, action='store', help='output files, but don''t wait for completion') 
    parser.add_argument('-oc', nargs=1, action='store', help='wait for completion, output files to be concatenated') 
    parser.add_argument('-och', nargs=1, action='store', help='wait for completion, output files to be concatenated, skip header on file 2,3,...') 
    #parser.add_argument('-pf',  action='count', help='pipe output file as input to the next script') 
    #DEFAULT: parser.add_argument('-pi', action='count', help='pipe job-id only to the next script') 
    parser.add_argument('-w', action='count', help='wait for completion, and show stdout') 
    parser.add_argument('-q', action='count', help='quiet, be as quiet as possible') 
    parser.add_argument('-v', action='count', help='be verbose, tell me more than normal') 
    parser.add_argument('args', nargs=argparse.REMAINDER)

    args= parser.parse_args() 
    if args.v: print(args)

        
    #    if len(sys.argv)<4:
    #print "Usage: %s [inputfile1] [inputfile2] [..] [-o outputfile1 [outputfile2] [..]] prog var1 [var2] [var3] .." % sys.argv[0]
    #    sys.exit(2)

    # pattern: multiple datafiles + program file + multiple variations
    # the program file ends with either ".R", ".m", or ".py" (that's the way it's recognized as a program

    # step 0: what do we convey as parameters to the script ? 
    # the job-id will ALWAYS be the first parameter passed to the script (you can't change that)
    convey_input_filenames=False
    convey_output_filenames=False

    # step 1: the program file, at least one
    if args.x==None or len(args.x)<1: 
        print "Need at least 1 program file to execute (an R or python script). eg. '-e YOURSCRIPT.R'"
        sys.exit(2)
    program_name=args.x[0]
    if not args.q: print "program -------- %s " % (program_name)
    if not os.path.exists( program_name): 
        print "Program %s not found!" % program_name 
        sys.exit(2)

    if args.dry==None:
        add_to_tagmap("mprogram", program_name, True, args.q)  # Force put program source onto redis 

    program2_name=''
    if args.x2:
        program2_name=args.x2[0]
        if not os.path.exists( program2_name): 
            print "Program2 %s not found!" % program2_name 
            sys.exit(2)
        if args.dry==None: 
            add_to_tagmap("mprogram", program2_name, True, args.q)  # Force put program source onto redis 

    # the input files
    infile_ls=[] 
    if args.ioe: 
        infile_ls=args.ioe
        # input filenames to be passed to the script as args, unless explicitly indicated
        if not args.cn:
            convey_input_filenames=True
    elif args.iae: 
        infile_ls=args.iae
    # put them on redis (if not yet there)
    for fn in infile_ls:
        if not args.q: print "inputfile ------ %s " % (fn)
        if args.dry==None:
            add_to_tagmap("mdata", fn, False, args.q) # put data onto redis, but don't force it

    # the output files
    wait_for_output=False # should this program wait for the output?
    concat_output=False   # should the output automatically be concatenatned?
    concat_skip_header=False   # when concatenating, skip 1 headerline for file 2,3,.. 
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
    elif args.w:   # not output file(s) follow, but wait for completion and show output 
        wait_for_output=True

    if not args.q: 
        for fn in outfile_ls:
            print "outputfile ----- %s " % (fn)

    # expand the variations
    variation_ls=[] 
    if args.x:
        for var in args.x[1:]:
            variation_ls.extend(expand_variation(var)) # extend = append vector to vector

    # what to convey to the script
    convey="none"
    if convey_input_filenames and convey_output_filenames:
        convey="both"
    elif convey_input_filenames:
        convey="input"
    elif convey_output_filenames:
        convey="output"
    
    # the jobs
    jobs_added_ls=[]
    if args.ioe:    # one input file for each script, variations are ignored
        for infile in infile_ls: 
            if not args.q: print "infile    ------ %s" % infile
            jobs_added_ls.append( add_job(program_name,[infile],outfile_ls, '', convey) ) # infile name is going to be passed as arg
    elif args.iae:  # all input files for each script         
        for var in variation_ls:
            if not args.q: print "variation ------ %s" % var
            jobs_added_ls.append( add_job(program_name,infile_ls,outfile_ls, var, convey) ) # infile name is going to be passed as arg
    else:           # no input files
        for var in variation_ls:
            if not args.q: print "variation ------ %s" % var
            jobs_added_ls.append( add_job(program_name,infile_ls,outfile_ls, var, convey) ) # infile name is going to be passed as arg


    l=len(jobs_added_ls)
    if l<25:
        if not args.q: print "Added jobs: {}".format(jobs_added_ls)
    else:
        if not args.q: print "Added jobs: {} {} .. {} {} ".format(jobs_added_ls[0],jobs_added_ls[1],
            jobs_added_ls[l-2] ,jobs_added_ls[l-1])

    concat_output_files=False
    if args.oc: concat_output_files=True

    concat_output_files_skip_header=False
    if args.och: concat_output_files_skip_header=True


    # BUG IN THE FOLLOWING: multiple output files are allowed, but not all handled!!! (only 1 is)
    # step 4: poll for the output, in case we have output files
    if len(outfile_ls)>0 and wait_for_output: 
        if not args.q: print "Waiting for output files: {} ".format(",".join(outfile_ls))
        job_waited_on_set=set(jobs_added_ls)
        report_set_len=True
        collected_file_ls=[]
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
                        collected_file_ls.append(fn)
                    # remove from set
                    job_waited_on_set.remove(jobid)
                    report_set_len=True
            sys.stdout.write(".") ; sys.stdout.flush()
            time.sleep(1) # sleep a second
        if concat_output_files or concat_output_files_skip_header: 
            output_lines=[]
            first=True
            for fn in sorted(collected_file_ls): 
                infile = file(fn,'r')
                lines_ls=infile.readlines()
                if not concat_output_files_skip_header: 
                    output_lines.extend(lines_ls) 
                elif first: 
                    output_lines.extend(lines_ls) 
                    first=False
                else:
                    output_lines.extend(lines_ls[1:]) 
                infile.close()
            outfile=file(outfile_ls[0],'w') 
            outfile.writelines(output_lines) 
            outfile.close()
                
    elif len(outfile_ls)==0 and wait_for_output:    # wait for completion and show output
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
                        o=re.sub( '\| $','', re.sub('\| +\|','||',   re.sub( '\n',' | ', ' | '.join(v)) ))
                    else:
                        o=v
                    print "%3d,%s: %s" % ( len(job_waited_on_set),jobid, o )

                    # remove from set
                    job_waited_on_set.remove(jobid)
            #sys.stdout.write(".") ; sys.stdout.flush()
            time.sleep(1) # sleep a second

    elif args.x2 :    # wait for completion of x, then kick off job x2
        job_waited_on_set=set(jobs_added_ls)
        report_set_len=True
        # wait for x to complete
        while (len(job_waited_on_set)>0):
            for job_json in r.smembers("sdone"): 
                jobattrib=json.loads(job_json)
                jobid=jobattrib['id'] 
                if jobid in job_waited_on_set:
                    print "%3d,%s" % ( len(job_waited_on_set), jobid )
                    job_waited_on_set.remove(jobid)
            #sys.stdout.write(".") ; sys.stdout.flush()
            time.sleep(1) # sleep a second
        # next stage: kick off x2, passing the jobid's of the completed jobs
        if not args.q: print "program2 ------- %s " % (program2_name)
        x2_job_added= add_job(program2_name,jobs_added_ls,outfile_ls, '', 'input') # infile names = job id's of completed jobs
        job_waited_on_set=set([x2_job_added])
        while (len(job_waited_on_set)>0):
            for job_json in r.smembers("sdone"): 
                jobattrib=json.loads(job_json)
                jobid=jobattrib['id'] 
                if jobid in job_waited_on_set:
                    print "%3d,%s" % ( len(job_waited_on_set),jobid )
                    job_waited_on_set.remove(jobid)
            #sys.stdout.write(".") ; sys.stdout.flush()
            time.sleep(1) # sleep a second
        # job is done, get the output file
        fn="{}-{}".format(x2_job_added, outfile_ls[0]) 
        content=r.hget("mdata",fn)
        write_file(outfile_ls[0],content)
         

    # THE END  
    if not args.q: print "\nDone"



def add_job(program_name, infile_ls, outfile_ls, variation_name, convey): 
        # get a job_id
        sq=r.incr("seq_job")
        tm=r.time()  # unix timestamp + microseconds
        jobid="j{}".format(sq)
        # push on the qjob queue
        r.lpush( "qjob", '{' + str.format( 
            r'''"id":"{0}","program":"{1}", "infile_ls":{2}, "outfile_ls":{3}, "variation":"{4}",
                "queue_time":"{5}.{6}", "convey":"{7}"''', 
            jobid, program_name, json.dumps(infile_ls), json.dumps(outfile_ls), variation_name, 
            tm[0],tm[1], convey ) + '}'  )
        return jobid


def add_to_tagmap(map_name,field,force,bequiet):
    # first check if the tag already exists, unless we are forced to put it onto redis
    if (not force) and (r.hexists(map_name,field)):
        if not bequiet: print "  map '%s' already contains '%s' " % ( map_name, field ) 
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
