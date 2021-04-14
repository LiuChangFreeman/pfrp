import socket
import os
import struct
import select

def padding(str):
    length=len(str)
    if length<32:
        return str+"0"*(32-length)
    else:
        return str[:32]

password="pfrp"
pid=os.getpid()
pack=struct.pack("?i33p",False, pid, padding(password))
result=struct.unpack( "?i33p" ,  pack )
print(padding(password)==result[-1])
print(result,len(pack))

temp={}
print(temp.values())
