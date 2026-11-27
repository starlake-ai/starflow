select {{ graph.nodes.values() | selectattr('resource_type', 'equalto', 'seed') | list | length }} as seed_count
