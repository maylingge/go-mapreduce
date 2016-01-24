# go-mapreduce
A simple framework to do map-reduce work
component:
  Master: responsible for spliting job to small partitions
          dispatch each task to registed workers
          and redispatch task for failed workers
          finally, merge all the result into one file
          
  Worker: know how to do Map and how to do Reduce
          communicate with master via RPC in both way
  
  
  main:   start up master and workers
  

key issues:
1. How workers register to master
2. How master dispatch task to worker
3. How master handles the worker failure
4. How master and worker communicate information
