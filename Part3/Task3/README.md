Task 3

This task runs a PageRank algorithm with different number of partitions and executor memory sizes with caching intermediate data.

How to run

$ ./run.sh [options] [num_partition] [memory_size_in_GB] [host_IP]
$ 	Options: 
$	    [input_name]: pass either web-BerkStan or enwiki-pages-articles
$	    [clear]: cleans up the output files written in hdfs directory

    The script runs Task3pagerankcache.py with two different datasets, i.e., Berkeley-Stanford web graph and enwiki-pages-articles.
