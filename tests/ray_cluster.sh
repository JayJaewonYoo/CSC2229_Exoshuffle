# This is a test file for running an on-premise Ray cluster. That is, to run on
# bare metal machines, or in a private cloud. Since there are two ways, we
# prefer not setting up the Ray cluster mannually on each node, but start the
# Ray cluster using the cluster-launcher.
#
# Reference:
# https://docs.ray.io/en/latest/cluster/vms/user-guides/launching-clusters/on-premises.html#manual-setup-cluster
# https://docs.ray.io/en/latest/cluster/running-applications/job-submission/quickstart.html#jobs-quickstart


#!/bin/bash

echo "The test for Ray cluster!"

conda activate ray

# TODO 1: Update the example-full.yaml to update head_ip, worker_ips, and
# ssh_user!!!

# Create or update the cluster. When the command finishes, it will print
# out the command that can be used to SSH into the cluster head node.
ray up example-full.yaml

# Get a remote screen on the head node.
ray attach example-full.yaml

# TODO 2: Try running a Ray program!!!
ray job submit --working-dir your_working_directory -- python ray_core.py

# Tear down the cluster.
ray down example-full.yaml
