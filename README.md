# CSC2229_Exoshuffle
CSC2229 course project implementing, evaluating, and innovating upon Exoshuffle. 

## Installation

The first step is to create a new conda environment, and install the latest
official version (2.9.3) of Ray from PyPI on Ubuntu servers (20.04.6 LTS). Note
that `ray[default]` contains three components of Ray: Core, Dashboard, and
Cluster Launcher.



```
conda create -y -n ray python=3.9
conda activate ray
pip install -U "ray[default]==2.9.3"
```


## References

[Ray Document](https://docs.ray.io/en/latest/index.html)