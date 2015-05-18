# Redis objects

## qjob (queue)

- job queue that receives the jobs from the client (the client 'pushes'), and that dishes out the jobs to the morsels (the morsels actually 'pop' the job from the queue)


## sprogress (set) 

- set that lists the jobs that are being processed at this very moment
- 

## sdone (set) 

- set that lists the jobs that have been completed 


## sflag (set)

- maintenance/signaling flags  
- flags are checked by morsels at the start of every cycle 
- eg to allow graceful shutdown


## mdata (map)

- data objects, both input as output


## moutput (map)

- contains the output produced by the morsels (ie. captures what the morsels printed to stdout and stderr)


## seq_job (sequence) 

Sequence used in numbering the jobs




