# Libvirt storage driver for CoreCluster
This is Libvirt(NFS) storage driver for CoreCluster cloud.

# Configuration
Update agents.py in CoreCluster to provide proper support of node, image and
storage tasks.

Update /etc/corenetwork/config.py and enable all libvirt drivers on nodes and management machine.

Update /etc/corenetwork/config.py and edit app.py file to enable hooks.
