MODULE = {
    'agents': [
        {'type': 'image', 'module': 'corecluster-storage-libvirt.agents.image_libvirt', 'count': 4},
        {'type': 'node', 'module': 'corecluster-storage-libvirt.agents.node_libvirt', 'count': 4},
        {'type': 'storage', 'module': 'corecluster-storage-libvirt.agents.storage_libvirt', 'count': 4},
    ],
    'drivers': {
        
    },
}
