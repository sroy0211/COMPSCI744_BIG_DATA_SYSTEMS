#!/bin/bash

# The overall command for running the run_part2.sh is below:-
# sudo docker exec master spark-submit <python file> <hdfs path for the dataset> <output folder name>

# The sample command is below:-
sudo docker exec master spark-submit part2.py hdfs://nn:9000/data/ hdfs://nn:9000/part2_output
