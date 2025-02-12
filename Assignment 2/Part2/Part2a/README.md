# Part2a

Part2a involves using Distributed Data Parallel Training with sync gradient with gather and scatter call using Gloo backend and a guide for running the code.

The steps for running the code for Part2a are below:-

## Step 1:- Use the command below in the directory where the main.py is present and as it is needed to run for each four nodes seperately.

The command is:-

python main.py --master-ip <ip of master node> --num-nodes 4 --rank <rank of the node>

### The sample command is below:-

python3 main.py --master-ip 10.10.1.1 --num-nodes 4 --rank 0 - For Node 0
python3 main.py --master-ip 10.10.1.1 --num-nodes 4 --rank 1 - For Node 1
python3 main.py --master-ip 10.10.1.1 --num-nodes 4 --rank 2 - For Node 2
python3 main.py --master-ip 10.10.1.1 --num-nodes 4 --rank 3 - For Node 3

### As we are training on all the four nodes, we give the two arguments of ip of master node and number of nodes same on all four nodes and we set the rank of nodes using the rank argument and the command needs to run seperately on all the four nodes in the terminal.
