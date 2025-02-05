#!/bin/bash

# The overall command for running the run_task2.sh is below:-
# sudo docker exec master spark-submit <python file> <hdfs path for the dataset> <output folder name> <number of partitions> <Host IP>

# The sample command is below:-
sudo docker exec master spark-submit task2.py hdfs://nn:9000/data1/ hdfs://nn:9000/part_3_task_2_data1_partition_1 1 3G hdfs://10.10.1.1:9000
