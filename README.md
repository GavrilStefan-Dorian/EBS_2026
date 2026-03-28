# Homework 1

## Parallelization

The program uses threads for parallelism, and two configs were tested:
* 1 thread
* 4 threads

## Number of generated messages

Two tests ran:
* 10000 publications and subscriptions
* 200000 publications and subscriptions

## Execution times 

| Publications | Subscriptions | Threads | Planning Time | Publication Generation Time | Subscription Generation Time | File Write Time | Total Time |
|-------------:|--------------:|--------:|--------------:|----------------------------:|-----------------------------:|----------------:|-----------:|
| 10000 | 10000 | 1 | 0.030514 s | 0.064062 s | 0.037328 s | 0.023053 s | 0.154958 s |
| 10000 | 10000 | 4 | 0.022684 s | 0.017471 s | 0.011464 s | 0.019037 s | 0.070656 s |
| 200000 | 200000 | 1 | 0.406678 s | 0.134954 s | 0.140904 s | 0.086859 s | 0.769394 s |
| 200000 | 200000 | 4 | 0.119982 s | 0.052584 s | 0.027126 s | 0.095179 s | 0.294870 s |


## Processor specifications
Processor: **[AMD Ryzen 7 260](https://www.cpubenchmark.net/cpu.php?cpu=AMD+Ryzen+7+260&id=6658)**

**NOTE**: here you can also add your own cpu and specs after testing, same for the exec runtimes

![ryzen7-260.png](ryzen7-260.png)
