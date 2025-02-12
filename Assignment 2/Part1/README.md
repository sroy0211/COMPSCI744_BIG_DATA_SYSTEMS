# Part1

Part1 involves training VGG-11 on CIFAR-10 and a guide for running the code.

The steps for running the code for Part1 are below:-

## Step 1:- Use the command below in the directory where the main.py is present and as it is for single node can be run on any of the four nodes.

The command is:-

python main.py --master-ip [ip of master node] --num-nodes 4 --rank [rank of the node]

### The sample command is below:-

python3 main.py 

### As we are training on a single node, we do not need the other arguments such as ip of master node, number of nodes and the rank of node in Part1.
