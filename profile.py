# Based on https://github.com/emulab/my-profile/blob/master/profile.py and https://docs.cloudlab.us/geni-lib.html

"""This is a gitrepo-based CloudLab profile for Jay Yoo and Kai Shen's work on their CSC2229 project; The profile source code and other software, documentation, etc. are stored in in a publicly accessible GIT repository (say, github.com). When you instantiate this profile, the repository is cloned to all of the nodes in your experiment, to `/local/repository`. 

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


# Variable number of nodes.
pc.defineParameter("nodeCount", "Number of Nodes", portal.ParameterType.INTEGER, 4,
                   longDescription="If you specify more then one node, " +
                   "we will create a lan for you.")


# Optional physical type for all nodes.
pc.defineParameter("phystype",  "Optional physical node type",
                   portal.ParameterType.STRING, "",
                   longDescription="Specify a single physical node type (pc3000,d710,etc) " +
                   "instead of letting the resource mapper choose for you.")

params = pc.bindParameters()
# Check parameter validity.
if params.nodeCount < 1:
    pc.reportError(portal.ParameterError("You must choose at least 1 node.", ["nodeCount"]))

if params.phystype != "":
    tokens = params.phystype.split(",")
    if len(tokens) != 1:
        pc.reportError(portal.ParameterError("Only a single type is allowed", ["phystype"]))

pc.verifyParameters()

if params.nodeCount > 1:
    if params.nodeCount == 2:
        lan = request.Link()
    else:
        lan = request.LAN()
    pass

# Add raw PCs to the request.
num_nodes = params.nodeCount
# nodes = []
 
# Variables initializing for defining data generation
max_number_of_data = 10000
interval = max_number_of_data / num_nodes

for i in range(num_nodes):
    curr_node = request.RawPC("node" + str(i))
    curr_node.disk_image = 'urn:publicid:IDN+emulab.net+image+emulab-ops//UBUNTU22-64-STD' # UBUNTU 22.04 as per the source code of the small-lan:37 profile
    # node.hardware_type = '' # "Specify a single physical node type (pc3000,d710,etc) instead of letting the resource mapper choose for you."
        # Commented out for now beacause we aren't asking for a specific machine yet

    # Install GenSort to the current node, still has to be opened using "tar xf FILENAME" in the execute script, and then probably has to be directly run on the execute script as well
        # NOTE: have to make sure if this is to be installed on only one node or all of the nodes
    curr_node.addService(pg.Install(url='http://www.ordinal.com/try.cgi/gensort-linux-1.5.tar.gz', path='/local')) # The documentation 
    curr_node.addService(pg.Install(url='https://github.com/JayJaewonYoo/CSC2229_Exoshuffle/blob/main/startup_script.tar.gz', path='/local')) # The documentation 

    # Install and execute a script that is contained in the repository.
        # start and end are arguments to define gensort data to generate at each node
    start = int(interval * i)
    end = int((i + 1) * interval - 1)
    curr_node.addService(pg.Execute(shell="sh", command="/local/startup_script.sh " + str(start) + " " + str(end)))

    # nodes.append(curr_node) # reference profile uses LAN for more than 2 nodes and always uses interfaces so I'll do the same here instead of just establishing a simple link
    if num_nodes > 1:
        iface = curr_node.addInterface("eth1")
        lan.addInterface(iface)
        pass

    if params.phystype != "":
        node.hardware_type = params.phystype
        pass

# Create a link between them
# link1 = request.Link(members = nodes)

# Print the RSpec to the enclosing page.
pc.printRequestRSpec(request)
