Colmo
=====

Colluding Morsels

A bunch of python scripts that work with [redis](http://redis.io/) to create a simple but effective distributed processing system. 

![](pipeline.png)

Sidenote: a [history](history.md) too short to be clicked 

## Demonstration 

These are the actions undertaken on the client.

The python program we are going to run ('main.py') goes into a loop, and keeps on generating random numbers until it finds one that ends with a requested pattern. 

Eg. you want to get a random number that ends with 12345 

eg. 
    $ ./main.py 12345
    result:46312345 after 114951 iterations

Of course the longer the pattern, the more iterations (and the longer) it takes to find a random number with the requested pattern. 
And if you are wondering: what is the use of this program? Sorry, it's completely useless, it's just a simple way to waste time! 

Here's the source of that program:

    #!/usr/bin/python 

    import sys
    import random

    if len(sys.argv)<2:
        print "Usage: %s pattern " % sys.argv[0]
        print "   eg. %s 7777 " % sys.argv[0]
        sys.exit(1)

    pattern=sys.argv[1]
    cnt=0
    random.seed()
    go_on=True
    while go_on:
        s=str.format("{0}",random.randrange(0,100000000))
        if s.endswith(pattern):
            print "result:%s after %d iterations" % ( s, cnt )
            go_on=False
        cnt=cnt+1

Let's now run this program on our *Colmo cluster*! 

Push the job and it's three variations onto the job queue: 

    push_job.py main.py nodata 11111 33333 55555 
    Adding  job266
    Adding  job267
    Adding  job268

Show me what the progress is: 

    lsp.py 
    === PROGRESS ============== 
    --- QUEUE: qjob ------- 
    {id:job268,program:main.py, data:nodata, variation:55555 ..} 
    {id:job267,program:main.py, data:nodata, variation:33333 ..}
    {id:job266,program:main.py, data:nodata, variation:11111 ..}
    --- SET: sprogress ------- 

Note: above json was slightly edited to make it more pallatable! 

A moment later, enquire again:

    lsp.py 
    === PROGRESS ============== 
    --- QUEUE: qjob ------- 
    {id:job268,program:main.py, data:nodata, variation:55555, ..}
    {id:job267,program:main.py, data:nodata, variation:33333, ..}
    --- SET: sprogress ------- 
    {id:job266,program:main.py, data:nodata, variation:11111, ..}


And a little while later, the job queue and the progress set are empty. 

    === PROGRESS ============== 
    --- QUEUE: qjob ------- 
    --- SET: sprogress ------- 

Now show the output of the jobs:

    $ showo.py 

    --- MAP: moutput ------- 
    job266 : result:40000000 after 7593693 iterations | 
    job267 : result:96633333 after 293774 iterations | 
    job268 : result:70000000 after 6718647 iterations | 


