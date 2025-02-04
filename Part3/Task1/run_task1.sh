#!/bin/bash

# The overall command for running the run_task1.sh is below:-
# sudo docker exec master spark-submit <python file> <hdfs path for the dataset> <output folder name>

# The sample command is below:-
sudo docker exec master spark-submit task1.py hdfs://nn:9000/data1/ hdfs://nn:9000/part3_task1
