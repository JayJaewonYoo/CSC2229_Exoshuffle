# CSC2229_Exoshuffle
CSC2229 course project implementing, evaluating, and innovating upon Exoshuffle. 

## Installation

The first step is to create a new `conda` environment with Python 3.9 using the
command:

```
conda update conda -y
conda create -n ray -y python=3.9
conda activate ray
```
where `ray` is the preferred name of your new environment.

The next step is to install the required Python packages, including the the latest
official version (2.9.3) of Ray. Note that `ray[default]` contains three
components of Ray: Core, Dashboard, and Cluster Launcher.

```
pip install -Ur requirements.txt
```

## Getting Started with Ray

Ray Core provides a small number of core primitives (i.e., tasks, actors,
objects) for building and scaling distributed applications. We have provided
`ray_core.py` for getting started with Ray and evaluating the installation.

```
python tests/ray_core.py
```
and the sample printed results are as follows:
```
2024-03-12 22:25:50,911 INFO worker.py:1715 -- Started a local Ray instance. View the dashboard at 127.0.0.1:8265 
[0, 1, 4, 9, 16]
10
10000.0 1000000.0
```


## References

[Ray Document](https://docs.ray.io/en/latest/index.html)