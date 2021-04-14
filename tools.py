def padding(str):
    length=len(str)
    if length<32:
        return str+"0"*(32-length)
    else:
        return str[:32]

def peek_pair_connections_pool(available_forward_connections):
    connections_pool_groups=available_forward_connections.values()
    if len(connections_pool_groups)>0:
        return max(connections_pool_groups,key=lambda pool:len(pool))
    else:
        return []