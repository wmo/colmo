#!/usr/bin/python

import json
import redis
import sys 
import urllib
import re

json_txt = urllib.urlopen("http://localhost/rome.json").read()
conf=json.loads(json_txt) 
r=redis.StrictRedis(host=conf["redisserver"], port=conf["redisport"],db=0)

def main(): 
    print "\n=== DATA ============== "
    print "--- MAP: mdata ------- "
    # suss out WHICH FILES
    file_ls=[]
    if len(sys.argv)==2 and sys.argv[1].endswith(":"):  # eg. getjdata 500: 
        num=get_job_id_num(re.sub(":$","",sys.argv[1]))
        for fn in r.hkeys('mdata'): 
            fnum=get_job_id_num(fn)
            if fnum>=num:
                file_ls.append(fn)
    elif len(sys.argv)>1:
        file_ls.extend(sys.argv[1:])
    else: 
        file_ls.extend(r.hkeys("mdata"))
    # let's get THOSE FILES
    for fn in file_ls:
        if fn.startswith("j"):
            content=r.hget("mdata",fn)
            print fn 
            write_file(fn,content)

def write_file(filename,content):
    if content==None:
        print "Warning: content==None %s" % filename
        return
    if content and (len(content)<1): 
        print "Warning: no content for %s" % filename
        return
    fw = open(filename,"wb")
    fw.write(content)
    fw.close()


def get_job_id_num(s):
    rv=0
    if s.startswith('j'): 
        rv=int(re.sub('^j','', re.sub('-.*$','', s)))
    return rv


if __name__ == "__main__": main()

