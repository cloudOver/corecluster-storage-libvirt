MODULE = {
    'agents': [
        {'type': 'image', 'module': 'corecluster.agents.image_libvirt', 'count': 4},
        {'type': 'node', 'module': 'corecluster.agents.node_libvirt', 'count': 4},
        {'type': 'storage', 'module': 'corecluster.agents.storage_libvirt', 'count': 4},
    ],
    'drivers': {
        
    },
}
