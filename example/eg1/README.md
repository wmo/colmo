# Find a patter in a random number

## Run it locally 

This timewaster of a program we are going to run, will keep running around in a loop, generating random numbers until it finds one that ends with the requested pattern. 

Eg. try out 'pattern_in_random.py'. Suppose you want to get a random number that ends with 9999 : 

    ./pattern_in_random.py 9999
    Python: 11394 iterations, found 74559999

Now do the same with the Octave program: 

    octave -q  pattern_in_random.m 9999
    Octave: 2199 iterations, found 1649999

And now in R: 

    R --slave --vanilla --quiet -f pattern_in_random.R --args 9999
    [1] "R: 2370 iterations, found 14269999"

You can already tell that the octave version is the slower one. Let's try it out on the cluster!


## Run it on the cluster 

Now we are going to need the scripts to talk to redis, and in case you haven't checked them out from github yet:

    git clone https://github.com/wmo/colmo 
    cd colmo 
    export PATH=$PATH:.

    cd example/eg1

## Cleanup 

This step can be skipped if you have just started up your cluster.

First we are goind to send a command to 'remove any old program, data, .. files' on each every morsel in the cluster. The problem we have is that we may not know how many morsels we have running in the cluster: this is because of the nature of the beast!

If you know how many morsels are running (eg 4), then just issue the command: 

    qrm.py 4

But if you don't know how many then it's better to err on the "too-many" side. Eg. I think i have 5 morsels running but it maybe 8. 

    qrm.py 8 

Now go grab a coffee, or do something else while you wait a while (1 minute, 1 hours, 1 day ??), until there is not a single job remaining in the job queue (qjob) nor in the progress set (sprogress) : 

    $ lsp.py 

    === PROGRESS ============== 
    --- QUEUE: qjob ------- 
    --- SET: sprogress -------


**TODO** rewrite above? 


After that is done, let's also clean up any data of older jobs stored in redis:
    
    dela.py 
  
## Launch the jobs  

Push the jobs on the colmo cluster: 


    $ qjob.py  pattern_in_random.py nodata 4444 55555 666666
    Adding  job641
    Adding  job642
    Adding  job643

    $ qjob.py  pattern_in_random.m nodata 4444 55555 666666 
    Adding  job644
    Adding  job645
    Adding  job646

    $ qjob.py  pattern_in_random.R nodata 4444 55555 666666 
    Adding  job647
    Adding  job648
    Adding  job649




----------------------------------------------------------------------
----------------------------------------------------------------------
----------------------------------------------------------------------
----------------------------------------------------------------------
# Launch 

Run 'pattern_in_random.m' standalone:

    octave -q pattern_in_random.m 0111
    Looking for:0111
    Found:18700111 after 1351 iterations


In case you haven't checked out the scripts from github yet:

    git clone https://github.com/wmo/colmo 
    cd colmo 
    export PATH=$PATH:.


    cd example/eg1

Launch the example: 

    qjob.py pattern_in_random.m  nodata 1 22 333 4444 55555 666666 7777777 88888888

    Adding  job611
    Adding  job612
    Adding  job613
    ..
    
----------------------------------------------------------------------
----------------------------------------------------------------------
----------------------------------------------------------------------
# Launch 

Run 'pattern_in_random.m' standalone:

    octave -q pattern_in_random.m 0111
    Looking for:0111
    Found:18700111 after 1351 iterations


In case you haven't checked out the scripts from github yet:

    git clone https://github.com/wmo/colmo 
    cd colmo 
    export PATH=$PATH:.


    cd example/eg1

Launch the example: 


    qjob.py pattern_in_random.m  nodata 1 22 333 4444 55555 666666 7777777 88888888

    Adding  job611
    Adding  job612
    Adding  job613


