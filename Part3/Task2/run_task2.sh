#!/bin/bash

# The overall command for running the run_task2.sh is below:-
# sudo docker exec master spark-submit <python file> <hdfs path for the dataset> <output folder name>

# The sample command is below:-
sudo docker exec master spark-submit task2.py hdfs://nn:9000/data1/ part_3_task_2
