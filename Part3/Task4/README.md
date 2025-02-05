# Task 4

Task 4 involves implementing a PageRank algorithm with a different number of partitions and executor memory sizes with caching intermediate data and it is particularly designed to see the effects of a worker process kill during the execution with a guide for running the code.

The steps for running the code for Task 4 are below:-

## Step 1:- After completing Part0, Part1, and Part2, once the environment is set, the dataset is copied to the hdfs using the command below:-

1. sudo docker exec -it [container id of hdfs namenode] bash
2. hdfs dfs -put [dataset to be copied to the hdfs] hdfs://nn:9000/[path to the folder to save the dataset inside hdfs]

   2.1. The sample command is below:-

      hdfs dfs -put export.csv hdfs://nn:9000/data

3. Then exit the container.
   
## Step 2:- Once the dataset is copied to the hdfs, we will run the .sh file appropriately with the steps shown below:-

1. The Python file needs to be copied to the spark master container to run the .sh file with the following command below:-

   sudo docker cp [python file name that is needed to be copied inside spark master container] [spark master container id]:/spark-3.3.4-bin-hadoop3/conf/[python file name that is being copied to the spark master container]

   1.1. The sample command is below:-

      sudo docker cp task4.py 2697ef6189d0:/spark-3.3.4-bin-hadoop3/conf/task4.py

2. Now run the .sh file using the following command given below:-

   sudo bash [the .sh file needed to run]

   2.1. The sample command is below:-

      sudo bash run_task4.sh

### The above code runs the Python file with two different datasets, web-BerkStan and enwiki-pages-articles, using the above commands and steps. To use both datasets, the file path needs to be changed appropriately in the .sh file to point out the location for the two datasets stored in hdfs, which can be taken from Step 1 above.

Once this is done, then we will execute the following command below once the application reaches 25% and 75% of its lifetime to trigger the failure to a worker container. The command is below:-

docker service scale service_name=2

The sample command is below:-
sudo docker service scale worker=2

The above command should be run in a different terminal and have to do the ssh first so that it is connected to the Nodes and then execute the command to kill the workers.
