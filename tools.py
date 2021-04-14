def padding(str):
    length=len(str)
    if length<32:
        result=str+"0"*(32-length)
    else:
        result=str[:32]
    if type(result)==unicode:
        result=result.encode("utf-8")
    return result

def peek_pair_connections_pool(available_forward_connections):
    connections_pool_groups=available_forward_connections.values()
    if len(connections_pool_groups)>0:
        return max(connections_pool_groups,key=lambda pool:len(pool))
    else:
        return []