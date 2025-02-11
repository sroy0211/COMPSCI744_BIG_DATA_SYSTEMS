# Part2

Part2 involves initializing spark sql instance, loading the input data, sorting it firstly by country code and then by timestamp, and writing the output in HDFS in CSV format and a guide for running the code.

The steps for running the code for Part2 are below:-

## Step 1:- After completing Part0, Part1, once the environment is set, the dataset is copied to the hdfs using the command below:-

1. sudo docker exec -it [container id of hdfs namenode] bash
2. hdfs dfs -put [dataset to be copied to the hdfs] hdfs://nn:9000/[path to the folder to save the dataset inside hdfs]

   2.1. The sample command is below:-

      hdfs dfs -put export.csv hdfs://nn:9000/data

3. Then exit the container.
   
## Step 2:- Once the dataset is copied to the hdfs, we will run the .sh file appropriately with the steps shown below:-

1. The Python file needs to be copied to the spark master container to run the .sh file with the following command below:-

   sudo docker cp [python file name that is needed to be copied inside spark master container] [spark master container id]:/spark-3.3.4-bin-hadoop3/conf/[python file name that is being copied to the spark master container]

   1.1. The sample command is below:-

      sudo docker cp part2.py 2697ef6189d0:/spark-3.3.4-bin-hadoop3/conf/part2.py

2. Now run the .sh file using the following command given below:-

   sudo bash [the .sh file needed to run]

   2.1. The sample command is below:-

      sudo bash run_part2.sh

### The above code runs the Python file with two different datasets, web-BerkStan and enwiki-pages-articles, using the above commands and steps. To use both datasets, the file path needs to be changed appropriately in the .sh file to point out the location for the two datasets stored in hdfs, which can be taken from Step 1 above.
