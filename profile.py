# Based on https://github.com/emulab/my-profile/blob/master/profile.py and https://docs.cloudlab.us/geni-lib.html

"""This is a trivial example of a gitrepo-based profile; The profile source code and other software, documentation, etc. are stored in in a publicly accessible GIT repository (say, github.com). When you instantiate this profile, the repository is cloned to all of the nodes in your experiment, to `/local/repository`. 

This particular profile is a simple example of using a single raw PC. It can be instantiated on any cluster; the node will boot the default operating system, which is typically a recent version of Ubuntu.

Instructions:
Wait for the profile instance to start, then click on the node in the topology and choose the `shell` menu item. 
"""

# Import the Portal object.
import geni.portal as portal
# Import the ProtoGENI library.
import geni.rspec.pg as pg

# Create a portal context.
pc = portal.Context()

# From the source code of the small-lan:37 profile
    # Shows how to specify options for OS image in the profile setup
'''
# Pick your OS.
imageList = [
    ('default', 'Default Image'),
    ('urn:publicid:IDN+emulab.net+image+emulab-ops//UBUNTU22-64-STD', 'UBUNTU 22.04'),
    ('urn:publicid:IDN+emulab.net+image+emulab-ops//UBUNTU20-64-STD', 'UBUNTU 20.04'),
    ('urn:publicid:IDN+emulab.net+image+emulab-ops//UBUNTU18-64-STD', 'UBUNTU 18.04'),
    ('urn:publicid:IDN+emulab.net+image+emulab-ops//CENTOS8S-64-STD',  'CENTOS 8 Stream'),
    ('urn:publicid:IDN+emulab.net+image+emulab-ops//FBSD123-64-STD', 'FreeBSD 12.3'),
    ('urn:publicid:IDN+emulab.net+image+emulab-ops//FBSD131-64-STD', 'FreeBSD 13.1')]

pc.defineParameter("osImage", "Select OS image",
                   portal.ParameterType.IMAGE,
                   imageList[0], imageList,
                   longDescription="Most clusters have this set of images, " +
                   "pick your favorite one.")
'''

# Create a Request object to start building the RSpec.
request = pc.makeRequestRSpec()
 
# Variables initializing for defining data generation
max_number_of_data = 10000
interval = max_number_of_data / 4

# Add a raw PC to the request.
num_nodes = 4
nodes = []
for i in range(num_nodes):
    curr_node = request.RawPC("node" + str(i))
    curr_node.disk_image = 'urn:publicid:IDN+emulab.net+image+emulab-ops//UBUNTU22-64-STD' # UBUNTU 22.04 as per the source code of the small-lan:37 profile
    node.hardware_type = '' # "Specify a single physical node type (pc3000,d710,etc) instead of letting the resource mapper choose for you."

    # Install GenSort to the current node, still has to be opened using "tar xf FILENAME" in the execute script, and then probably has to be directly run on the execute script as well
        # NOTE: have to make sure if this is to be installed on only one node or all of the nodes
    node.addService(pg.Install(url='http://www.ordinal.com/try.cgi/gensort-linux-1.5.tar.gz', path='/local')) # The documentation 

    # Install and execute a script that is contained in the repository.
        # start and end are arguments to define gensort data to generate at each node
    start = int(interval * i)
    end = int((i + 1) * interval - 1)
    node.addService(pg.Execute(shell="sh", command="/local/repository/startup_script.sh " + str(start) + " " + str(end)))

    nodes.append(curr_node)

# Create a link between them
link1 = request.Link(members = nodes)

# Print the RSpec to the enclosing page.
pc.printRequestRSpec(request)
