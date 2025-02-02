#!/bin/bash

if [[ "$@" = *--help ]] || [[ "$@" = *-h ]]; then
	echo "Usage: ./run.sh [options] [host_IP]"
	echo -e "\t[input_name] runs the pagerank with the input parameter (web-BerkStan | enwiki-pages-articles)"
	echo -e "\t[clear] cleans up the output files in hdfs directory"
	exit 1
fi

## Cleaning up resources
if [  "$1" = "clear" ]; then
	hdfs dfsadmin -safemode leave
	hdfs dfs -rm -r ${namenode_dir}/user/hcha/assignment1/part3/task1/web-BerkStan.out
	hdfs dfs -rm -r ${namenode_dir}/user/hcha/assignment1/part3/task1/enwiki-pages-articles.out
	hdfs dfsadmin -safemode enter
	exit 1	
fi

if [[ "$1" != "web-BerkStan" ]] && [[ "$1" != "enwiki-pages-articles" ]] || [[ "$2" = "" ]]; then
	echo "Usage: ./run.sh [options] [host_IP]"
	echo -e "\t[input_name] runs the pagerank with the input parameter (web-BerkStan | enwiki-pages-articles)"
	echo -e "\t[clear] cleans up the output files in hdfs directory"
	exit 1
fi


## Leaving hdfs safemode
hdfs dfsadmin -safemode leave

## The path for namenonde
namenode_dir=hdfs://10.10.1.1:9000

## Making the directory paths for the first time
hadoop fs -mkdir ${namenode_dir}/user
hadoop fs -mkdir ${namenode_dir}/user/hcha
hadoop fs -mkdir ${namenode_dir}/user/hcha/assignment1/part3/task1

## Seting up the assignment1 default path
assignment1_dir=${namenode_dir}/user/hcha/assignment1/part3/task1


## Collecting the stats before execution
mkdir logs
echo -e "---------- Network Stats Before -------------" >> logs/$1_namenode.stat
cat /proc/net/dev >> logs/$1_namenode.stat
echo -e "\n\n---------- Disk Stats Before -------------" >> logs/$1_namenode.stat
cat /proc/diskstats >> logs/$1_namenode.stat

echo -e "---------- Network Stats Before -------------" >> logs/$1_datanode1.stat
ssh node1 cat /proc/net/dev >> logs/$1_datanode1.stat
echo -e "\n\n---------- Disk Stats Before -------------" >> logs/$1_datanode1.stat
ssh node1 cat /proc/diskstats >> logs/$1_datanode1.stat

echo -e "---------- Network Stats Before -------------" >> logs/$1_datanode2.stat
ssh node2 cat /proc/net/dev >> logs/$1_datanode2.stat
echo -e "\n\n---------- Disk Stats Before -------------" >> logs/$1_datanode2.stat
ssh node2 cat /proc/diskstats >> logs/$1_datanode2.stat

sleep 2

driver_memory=30G
executor_memory=30G

## Running the pagerank application
if [ "$1" = "web-BerkStan" ]; then
	## add local input file (web-BerkStan.txt) to hdfs
	hdfs dfs -put ../web-BerkStan.txt ${assignment1_dir}/web-BerkStan.txt
	echo "Running pagerank with web-Berkstan.txt"
	spark-submit --master spark://$2:7077 --class "pagerank" --driver-memory ${driver_memory} --executor-memory ${executor_memory} --num-executors 2 --executor-cores 5 pagerank.py ${assignment1_dir}/web-BerkStan.txt ${assignment1_dir}/web-BerkStan.out >> logs/web-BerkStan.log
else
	## Adding the local input file to hdfs
	hdfs dfs -put /proj/uwmadison744-f21-PG0/data-part3/enwiki-pages-articles/ ${assignment1_dir}/enwiki-pages-articles
	echo "Running pagerank with enwiki-pages-articles"
	spark-submit --master spark://$2:7077 --class "pagerank" --driver-memory ${driver_memory} --executor-memory ${executor_memory} --num-executors 2 --executor-cores 5 pagerank.py ${assignment1_dir}/enwiki-pages-articles ${assignment1_dir}/enwiki-pages-articles.out >> logs/enwiki-pages-articles.log
fi

sleep 2

## Collecting the stats after execution
echo -e "---------- Network Stats After -------------" >> logs/$1_namenode.stat
cat /proc/net/dev >> logs/$1_namenode.stat
echo -e "\n\n---------- Disk Stats After -------------" >> logs/$1_namenode.stat
cat /proc/diskstats >> logs/$1_namenode.stat

echo -e "---------- Network Stats After -------------" >> logs/$1_datanode1.stat
ssh node1 cat /proc/net/dev >> logs/$1_datanode1.stat
echo -e "\n\n---------- Disk Stats After -------------" >> logs/$1_datanode1.stat
ssh node1 cat /proc/diskstats >> logs/$1_datanode1.stat

echo -e "---------- Network Stats After -------------" >> logs/$1_datanode2.stat
ssh node2 cat /proc/net/dev >> logs/$1_datanode2.stat
echo -e "\n\n---------- Disk Stats After -------------" >> logs/$1_datanode2.stat
ssh node2 cat /proc/diskstats >> logs/$1_datanode2.stat



## Reading the output file
if [ "$1" = "web-BerkStan" ]; then
	hdfs dfs -cat ${assignment1_dir}/web-BerkStan.out/* 2>/dev/null | head
else
	hdfs dfs -cat ${assignment1_dir}/enwiki-pages-articles.out/* 2>/dev/null | head
fi

## Entering the hdfs safemode
hdfs dfsadmin -safemode enter
