#!/usr/bin/python

import datetime
import glob
import json
import os
import os.path
import re
import redis
import socket
import sys
import time
import urllib
import zipfile

json_txt = urllib.urlopen("http://localhost/rome.json").read()
conf=json.loads(json_txt) 
r=redis.StrictRedis(host=conf["redisserver"], port=conf["redisport"],db=0)

popped_major_jobid_set=set()

def main(): 
    # change to working directory passed as arg 1 on the CLI. 
    if len(sys.argv)>1:
        working_dir=sys.argv[1]
        print "Changing to working dir: " + working_dir
        os.chdir(working_dir) 

    morsel_name=re.sub('^.*/','', os.getcwd()) 
    sleeptime=1 # 1 second
    while True:
        check_flags_for_exit(morsel_name) # check the flags, if we need to stop this morsel
        now = datetime.datetime.now()
        json_txt=r.rpop('qjob')
        if (json_txt is None):
            print "%10s: %02d:%02d:%02d nothing in queue, sleeping %ds" % (
                   morsel_name, now.hour, now.minute,now.second,sleeptime)
            time.sleep(sleeptime)
            sleeptime= min( sleeptime*2, 16) # maximum sleeptime = 16 seconds
        else:
            jobattrib=json.loads(json_txt) 
            jobid=jobattrib['id']   # eg. job147:3 -> major id=job147, minor=3
            print "%10s: %02d:%02d:%02d %s" % (morsel_name,now.hour,now.minute,now.second,jobid)
            # never do a job with the same major id twice!
            major_jobid=re.sub(':.*$','',jobid)
            if major_jobid in popped_major_jobid_set: 
                # push this job back on the queue, I'm not doing it, it's not for me
                json_txt=r.lpush('qjob',json_txt)
                print "%10s: -------- %s pushed back on queue, sleeping %ds" % (morsel_name,jobid,sleeptime) 
                time.sleep(sleeptime) # sleep before we try again
                sleeptime= min( sleeptime*2, 16) # maximum sleeptime = 16 seconds
                continue
            popped_major_jobid_set.add(major_jobid) 

            # is it a 'command' job ? 
            if 'command' in jobattrib: 
                cmd=jobattrib['command']
                # check if we need to do a housekeeping job
                if cmd=='cleanup': 
                    cleanup(morsel_name,jobid)
                    continue
                elif cmd=='ping': 
                    ping(morsel_name,jobid)
                    continue

            # it's a regular job
            program=jobattrib['program']
            infile_ls=jobattrib['infile_ls']
            outfile_ls=jobattrib['outfile_ls']
            variation=jobattrib['variation']

            #print "got {} infiles".format(len(infile_ls))
            #print "got {} outfiles".format(len(outfile_ls))

            # 1. add it to the progress set
            tm=r.time()  # unix timestamp + microseconds
            progress_txt=append_values_to_json(json_txt, str.format(r' "exec_start":"{0}.{1}"',tm[0],tm[1]) )
            r.sadd('sprogress',progress_txt) # add it to the 'sprogress' set

            # 2. execute the job 
            output=handle_job(morsel_name, program, jobid, infile_ls, outfile_ls, variation) 
            r.srem('sprogress', progress_txt) # remove it from the 'progress' set

            # 3. put output in the moutput map
            putout(jobid,output) 
            sleeptime=1

            # 4. read the output files and put in the mdata map 
            for fn in outfile_ls:
                if os.path.isfile(fn):
                    with open(fn, 'r') as content_file:
                        content = content_file.read()
                        # prepend the job-id the filename
                        dataname="{}-{}".format(jobattrib['id'],fn)
                        r.hset("mdata",dataname,content)
                        print "Added {} to mdata in redis".format(dataname)

            # 5. add to the done set
            tm=r.time()  # unix timestamp + microseconds
            done_txt=append_values_to_json(progress_txt, str.format(r' "exec_end":"{0}.{1}"',tm[0],tm[1]) )
            r.sadd('sdone',done_txt) 


def cleanup(morsel_name,jobid):
    print "%10s: -------- cleanup " % morsel_name
    workdir=os.getcwd()
    for root, dirs, files in os.walk(workdir):
        for f in files:
            os.unlink(os.path.join(root, f))
        for d in dirs:
            shutil.rmtree(os.path.join(root, d))
    putout( jobid, "cleanup " + socket.gethostname()+":"+re.sub('^.*/','', workdir))

def putout(jobid,output): 
    r.hset("moutput",jobid,json.dumps( { 'output':output } ) )


def ping(morsel_name,jobid):
    print "%10s: -------- ping " % morsel_name
    workdir=os.getcwd()
    files_n_dirs = glob.glob(workdir+"/*")
    putout( jobid, 
            str.format("pong from {0}:{1} ({2} files in working dir)", 
                socket.gethostname(),re.sub('^.*/','', workdir),  len(files_n_dirs) ) 
            )

# NOTE: this function uses popen, a deprecated function!!
# see https://docs.python.org/2/library/subprocess.html#subprocess-replacements
def handle_job(morsel_name,program_tag, job_id, infile_ls,outfile_ls,variation_tag):
    print "%10s: -------- program    : %s" % ( morsel_name, program_tag )
    print "%10s: -------- inputfiles : %s" % ( morsel_name, ",".join(infile_ls) )
    print "%10s: -------- outputfiles: %s" % ( morsel_name, ",".join(outfile_ls) )
    print "%10s: -------- variation  : %s" % ( morsel_name, variation_tag )
    #orig if r.hexists("mprogram",program_tag) and not(os.path.isfile(program_tag)):
    if r.hexists("mprogram",program_tag): # always copy the latest program file from redis
        content=r.hget("mprogram",program_tag)
        write_file(morsel_name,program_tag,content) 
    # copy the infiles
    for data_tag in infile_ls:
        if r.hexists("mdata",data_tag) and not(os.path.isfile(data_tag)):
            content=r.hget("mdata",data_tag)
            write_file(morsel_name,data_tag,content) 
    if r.hexists("mvariation",variation_tag) and not(os.path.isfile(variation_tag)):
        content=r.hget("mvariation",variation_tag)
        write_file(morsel_name,variation_tag,content) 
    # now execute the program 
    output=''
    if program_tag.endswith(".py"):
        pipe = os.popen('/usr/bin/python ' + program_tag + ' ' + job_id + ' ' + variation_tag)
        output=pipe.readlines()
    elif program_tag.endswith(".m"):
        pipe = os.popen('/usr/bin/octave -q ' + program_tag + ' ' +  job_id + ' ' + variation_tag)
        output=pipe.readlines()
    elif program_tag.endswith(".R"):
        exeStr='/usr/bin/Rscript ' + program_tag + ' ' +  job_id + ' ' + ' '.join(infile_ls) + ' ' + variation_tag
        print exeStr
        (child_stdin, pipe) = os.popen4(exeStr)
        #pipe = os.popen('/usr/bin/R --slave --vanilla --quiet -f ' + program_tag + ' --args ' + variation_tag)
        output=pipe.readlines()
    return output  


def write_file(morsel_name,filename,content):
    if (len(content)<1): return
    print "%10s: -------- writing file %s" % (morsel_name,filename)
    fw = open(filename,"wb")
    fw.write(content)
    fw.close()
    if filename.endswith(".zip"): 
        print "%10s: -------- unzipping file %s" % (morsel_name,filename)
        with zipfile.ZipFile(filename) as the_zip_file:
            the_zip_file.extractall() 
    if filename.endswith(".py"): 
        os.chmod(filename,0755)

def append_values_to_json(json_txt, appendix): 
    # hack: add content as of where } starts in the input json 
    n=str.rfind(json_txt,"}")
    if (n<0): 
        print "PANIC: no final } in json string. Apathic reaction, doing nothing!!"
    json_new=json_txt[0:n] + "," + appendix + " }"
    return json_new

def check_flags_for_exit(morsel_name):
    for v in r.smembers("sflag"):
        flagattrib=json.loads(v)
        if flagattrib["command"]=="suspend_host":
            if flagattrib["hostname"]== socket.gethostname():
                now = datetime.datetime.now()
                print "%10s: %02d:%02d:%02d exiting because of flag %s" % ( morsel_name, now.hour, now.minute,now.second,v)
                sys.exit(0)

if __name__ == "__main__": main()

