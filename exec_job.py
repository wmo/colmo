#!/usr/bin/python

import datetime
import glob
import json
import os
import os.path
import re
import redis
import socket
import time
import urllib
import zipfile

json_txt = urllib.urlopen("http://signpost/rome.json").read()
conf=json.loads(json_txt) 
r=redis.StrictRedis(host=conf["server"], port=conf["port"],db=0)

popped_major_jobid_set=set()

def main(): 
    sleeptime=1 # 1 second
    while True:
        now = datetime.datetime.now()
        json_txt=r.rpop('qjob')
        if (json_txt is None):
            print "%02d:%02d:%02d nothing in queue, sleeping %ds" % (now.hour, now.minute,now.second,sleeptime)
            time.sleep(sleeptime)
            sleeptime= min( sleeptime*2, 16) # maximum sleeptime = 16 seconds
        else:
            jobattrib=json.loads(json_txt) 
            jobid=jobattrib['id']   # eg. job147:3 -> major id=job147, minor=3
            print "%02d:%02d:%02d %s" % (now.hour,now.minute,now.second,jobid)
            # never do a job with the same major id twice!
            major_jobid=re.sub(':.*$','',jobid)
            if major_jobid in popped_major_jobid_set: 
                # push this job back on the queue, I'm not doing it, it's not for me
                json_txt=r.lpush('qjob',json_txt)
                print "-------- %s pushed back on queue, sleeping %ds" % ( jobid, sleeptime) 
                time.sleep(sleeptime) # sleep before we try again
                sleeptime= min( sleeptime*2, 16) # maximum sleeptime = 16 seconds
                continue
            popped_major_jobid_set.add(major_jobid) 

            # is it a 'command' job ? 
            if 'command' in jobattrib: 
                cmd=jobattrib['command']
                # check if we need to do a housekeeping job
                if cmd=='cleanup': 
                    cleanup(jobid)
                    continue
                elif cmd=='ping': 
                    ping(jobid)
                    continue

            # it's a regular job
            program=jobattrib['program']
            data=jobattrib['data']
            variation=jobattrib['variation']

            #print "*** %02d:%02d:%02d new job %s" % (now.hour, now.minute,now.second,jobid)
            # 1. add it to the progress set
            tm=r.time()  # unix timestamp + microseconds
            progress_txt=append_values_to_json(json_txt, str.format(r' "exec_start":"{0}.{1}"',tm[0],tm[1]) )
            r.sadd('sprogress',progress_txt) # add it to the 'sprogress' set

            # 2. execute the job 
            output=handle_job(program, data,variation) 
            r.srem('sprogress', progress_txt) # remove it from the 'progress' set

            # 3. add to the done set
            tm=r.time()  # unix timestamp + microseconds
            done_txt=append_values_to_json(progress_txt, str.format(r' "exec_end":"{0}.{1}"',tm[0],tm[1]) )
            r.sadd('sdone',done_txt) 

            # 4. put output in the moutput map
            putout(jobid,output) 
            sleeptime=1


def cleanup(jobid):
    print "-------- cleanup "
    workdir=os.getcwd()
    for root, dirs, files in os.walk(workdir):
        for f in files:
            os.unlink(os.path.join(root, f))
        for d in dirs:
            shutil.rmtree(os.path.join(root, d))
    putout( jobid, "cleanup " + socket.gethostname()+":"+re.sub('^.*/','', workdir))

def putout(jobid,output): 
    r.hset("moutput",jobid,json.dumps( { 'output':output } ) )

def ping(jobid):
    print "-------- ping " 
    workdir=os.getcwd()
    files_n_dirs = glob.glob(workdir+"/*")
    putout( jobid, 
            str.format("pong from {0}:{1} ({2} files in working dir)", 
                socket.gethostname(),re.sub('^.*/','', workdir),  len(files_n_dirs) ) 
            )


def handle_job(program_tag,data_tag,variation_tag):
    print "-------- program   : %s" % program_tag
    print "-------- data      : %s" % data_tag
    print "-------- variation : %s" % variation_tag
    if r.hexists("mprogram",program_tag) and not(os.path.isfile(program_tag)):
        content=r.hget("mprogram",program_tag)
        write_file(program_tag,content) 
    if r.hexists("mdata",data_tag) and not(os.path.isfile(data_tag)):
        content=r.hget("mdata",data_tag)
        write_file(data_tag,content) 
    if r.hexists("mvariation",variation_tag) and not(os.path.isfile(variation_tag)):
        content=r.hget("mvariation",variation_tag)
        write_file(variation_tag,content) 
    # now execute the program 
    output=''
    if program_tag.endswith(".py"):
        pipe = os.popen('/usr/bin/python ' + program_tag + ' ' + variation_tag)
        output=pipe.readlines()
    elif program_tag.endswith(".m"):
        pipe = os.popen('/usr/bin/octave -q ' + program_tag + ' ' + variation_tag)
        output=pipe.readlines()
    elif program_tag.endswith(".R"):
        pipe = os.popen('/usr/bin/R <' + program_tag + ' ' + variation_tag)
        output=pipe.readlines()
    return output  


def write_file(filename,content):
    if (len(content)<1): return
    print "Writing file %s" % filename
    fw = open(filename,"wb")
    fw.write(content)
    fw.close()
    if filename.endswith(".zip"): 
        print "Unzipping file %s" % filename
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

if __name__ == "__main__": main()
