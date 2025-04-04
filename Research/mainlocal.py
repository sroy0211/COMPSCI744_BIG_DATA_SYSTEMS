# Importing the necessary libraries
import argparse
import os
import torch
import json
import copy
import numpy as np
import torch.nn as nn
import torch.nn.functional as F
import torch.optim as optim
import logging
import random
import model as mdl
import time
import signal
import shutil
import torch.distributed as dist
from torchvision import datasets, transforms
from torch.utils.data.distributed import DistributedSampler
from torch.nn.parallel import DistributedDataParallel as DDP

# Setting up the parameters
device = "cpu"
torch.set_num_threads(4)
log_iter = 20
group_list = []
seed = 2021
torch.manual_seed(seed)
np.random.seed(seed)

# Global for SIGINT handling
received_sigint = False
checkpoint_path = "./checkpoint"
model_file = os.path.join(checkpoint_path, "model.pth")
optim_file = os.path.join(checkpoint_path, "optim.pth")

# Handle SIGINT signals for graceful checkpointing
def handle_sigint(signum, frame):
    global received_sigint
    print(f"[Rank {dist.get_rank()}] Received SIGINT. Preparing to checkpoint and exit.")
    received_sigint = True

signal.signal(signal.SIGINT, handle_sigint)

# Save model and optimizer state
def save_checkpoint(model, optimizer):
    os.makedirs(checkpoint_path, exist_ok=True)
    torch.save(model.state_dict(), model_file)
    torch.save(optimizer.state_dict(), optim_file)
    print(f"[Rank {dist.get_rank()}] Checkpoint saved.")

# Load model and optimizer state
def load_checkpoint(model, optimizer):
    if os.path.exists(model_file) and os.path.exists(optim_file):
        model.load_state_dict(torch.load(model_file))
        optimizer.load_state_dict(torch.load(optim_file))
        print(f"[Rank {dist.get_rank()}] Loaded checkpoint.")
        return True
    return False

# Attempt to sync checkpoint from peer
def sync_checkpoint_from_peer():
    for peer_rank in range(dist.get_world_size()):
        if peer_rank == dist.get_rank():
            continue
        try:
            peer_dir = f"/users/user{peer_rank}/checkpoint"
            if os.path.exists(os.path.join(peer_dir, "model.pth")):
                shutil.copy(os.path.join(peer_dir, "model.pth"), model_file)
                shutil.copy(os.path.join(peer_dir, "optim.pth"), optim_file)
                print(f"[Rank {dist.get_rank()}] Synced checkpoint from Rank {peer_rank}")
                return True
        except Exception as e:
            continue
    return False

# Training function
def train_model(model, train_loader, optimizer, criterion, epoch, rank):
    group = dist.new_group(group_list)
    group_size = len(group_list)
    start_time = time.time()
    log_iter_start_time = time.time()

    with open(f'output/{log_file_name}', 'a+') as f:
        for batch_idx, (data, target) in enumerate(train_loader):
            batch_count = batch_idx + 1
            if batch_idx >= stop_iter:
                break

            data, target = data.to(device), target.to(device)
            optimizer.zero_grad()
            output = model(data)
            loss = criterion(output, target)
            loss.backward()

            dist.barrier()
            if received_sigint:
                save_checkpoint(model, optimizer)
                dist.destroy_process_group()
                exit(0)

            optimizer.step()
            dist.barrier()

            elapsed_time = time.time() - start_time
            f.write(f"{epoch},{batch_count},{elapsed_time}\n")
            start_time = time.time()

            if (batch_count % log_iter == 0):
                log_iter_elapsed_time = time.time() - log_iter_start_time
                print('Train Epoch: {} \t Iteration: {} [{}/{} ({:.0f}%)]\tLoss: {:.6f}\t elapsed time: {:.3f}'.format(
                    epoch, batch_count, batch_count * len(data) * group_size, len(train_loader.dataset),
                    100. * batch_count / len(train_loader), loss.item(), log_iter_elapsed_time)) 
                log_iter_start_time = time.time()

# Testing function
def test_model(model, test_loader, criterion):
    model.eval()
    test_loss = 0
    correct = 0
    with torch.no_grad():
        for batch_idx, (data, target) in enumerate(test_loader):
            data, target = data.to(device), target.to(device)
            output = model(data)
            test_loss += criterion(output, target)
            pred = output.max(1, keepdim=True)[1]
            correct += pred.eq(target.view_as(pred)).sum().item()
    test_loss /= len(test_loader)
    print('Test set: Average loss: {:.4f}, Accuracy: {}/{} ({:.0f}%)\n'.format(
        test_loss, correct, len(test_loader.dataset),
        100. * correct / len(test_loader.dataset)))

# Init process
def init_process(master_ip, rank, size, fn, backend='gloo'):
    dist.init_process_group(backend, init_method=f"tcp://{master_ip}:6585",
                            rank=rank, world_size=size)
    fn(rank, size)

# Run training and testing
def run(rank, size):
    normalize = transforms.Normalize(mean=[x/255.0 for x in [125.3, 123.0, 113.9]],
                                     std=[x/255.0 for x in [63.0, 62.1, 66.7]])
    transform_train = transforms.Compose([
        transforms.RandomCrop(32, padding=4),
        transforms.RandomHorizontalFlip(),
        transforms.ToTensor(),
        normalize,
    ])
    transform_test = transforms.Compose([
        transforms.ToTensor(),
        normalize])

    training_set = datasets.CIFAR10(root="./data", train=True, download=True, transform=transform_train)
    sampler = DistributedSampler(training_set, seed=seed) if torch.distributed.is_available() else None
    train_loader = torch.utils.data.DataLoader(training_set,
                                               num_workers=2,
                                               batch_size=batch_size,
                                               sampler=sampler,
                                               pin_memory=True)
    test_set = datasets.CIFAR10(root="./data", train=False, download=True, transform=transform_test)
    test_loader = torch.utils.data.DataLoader(test_set,
                                              num_workers=2,
                                              batch_size=batch_size,
                                              shuffle=False,
                                              pin_memory=True)
    training_criterion = torch.nn.CrossEntropyLoss().to(device)
    model = mdl.VGG11().to(device)
    ddp_model = DDP(model)
    optimizer = optim.SGD(ddp_model.parameters(), lr=0.1, momentum=0.9, weight_decay=0.0001)

    checkpoint_found = load_checkpoint(ddp_model, optimizer)
    if not checkpoint_found:
        synced = sync_checkpoint_from_peer()
        if synced:
            load_checkpoint(ddp_model, optimizer)

    for epoch in range(num_epochs):
        train_model(ddp_model, train_loader, optimizer, training_criterion, epoch, rank)
    test_model(ddp_model, test_loader, training_criterion)

# Main entry point
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--master-ip', type=str, default='10.10.1.1', help='master node ip (default: 10.10.1.1)')
    parser.add_argument('--num-nodes', type=int, default=4, help='the number of nodes (default:4)')
    parser.add_argument('--rank', type=int, default=0, help='rank of node')
    parser.add_argument('--epoch', type=int, default=1, help='the number of epochs (default:1)')
    parser.add_argument('--stop_iter', type=int, default=40, help='Stop iteration at, (default: 40)')
    parser.add_argument('--total_batch_size', type=int, default=256, help='total batch size')
    args = parser.parse_args()

    global batch_size, num_epochs, stop_iter
    global log_file_name

    total_batch_size = args.total_batch_size
    batch_size = int(total_batch_size // args.num_nodes)
    num_epochs = args.epoch
    stop_iter = args.stop_iter
    master_ip = args.master_ip
    num_nodes = args.num_nodes
    rank = args.rank

    log_file_name = f"timelog_{num_epochs}_{stop_iter}_{num_nodes}_{batch_size}.csv"
    os.makedirs("output", exist_ok=True)
    with open(f'output/{log_file_name}', 'w+') as f:
        f.write("epoch,iteration,elpased_time\n")

    for group in range(0, num_nodes):
        group_list.append(group)

    init_process(args.master_ip, rank, num_nodes, run)