from enum import Enum
from tools import padding,peek_pair_connections_pool
import os
import socket
import struct
import select

class ConnectionStatus(Enum):
    established=0
    ready=1
    working = 2
    dead=3

class ConnectionType(Enum):
    server_forward_connection=0
    server_service_connection = 1
    client_forward_connection=2
    client_service_connection = 3

class CoreUtil():
    def __init__(self,data_config):
        self.log=None
        self.epoll=None
        self.data_config=data_config
        self.fd_2_socket_table = {}
        self.fd_2_connection_table = {}
        self.available_forward_connections = {}
        self.ready_service_connections = []
        self.forward_2_service_table = {}
        self.service_2_forward_table = {}

    def schedule(self):
        ready_service_connections=self.ready_service_connections
        for i in range(len(ready_service_connections) - 1, -1, -1):
            service_connection=ready_service_connections[i]
            connections_pool = peek_pair_connections_pool(self.available_forward_connections)
            if len(connections_pool)>0:
                self.log.debug("core_util schedule")
                fordward_connection=connections_pool.pop()
                del ready_service_connections[i]
                fordward_connection.status = ConnectionStatus.working
                service_connection.status = ConnectionStatus.working
                self.forward_2_service_table[fordward_connection.fd] = service_connection
                self.service_2_forward_table[service_connection.fd] = fordward_connection
                if len(service_connection.read_buffer)!=0:
                    self.log.debug("core_util request_upstream")
                    service_connection.request_upstream()

class Connection():
    def __init__(self,fd,socket,core_util):
        self.status=ConnectionStatus.established
        self.fd=fd
        self.connection_socket=socket
        self.read_buffer=""
        self.write_buffer=""
        self.core_util=core_util

    def send(self):
        connection_socket=self.connection_socket
        write_buffer=self.write_buffer
        try:
            length_done=connection_socket.send(write_buffer)
            self.write_buffer=write_buffer[length_done:]
        except socket.error as e:
            log=self.core_util.log
            log.warn("send msg error, abort")
            log.error(e)
            self.status=ConnectionStatus.dead

    def recv(self):
        log = self.core_util.log
        connection_socket=self.connection_socket
        try:
            temp_buffer=connection_socket.recv(4096)
            if temp_buffer:
                self.read_buffer += temp_buffer
            else:
                log.warn('connection close, abort')
                self.status = ConnectionStatus.dead
        except socket.error as e:
            log.warn("recv msg error, abort")
            log.error(e)
            self.status=ConnectionStatus.dead

    def close(self):
        fd=self.fd
        core_util=self.core_util
        core_util.epoll.unregister(fd)
        self.connection_socket.close()
        del core_util.fd_2_socket_table[fd]
        del core_util.fd_2_connection_table[fd]

class ForwardConnection(Connection):
    def __init__(self,fd,socket,core_util):
        Connection.__init__(self,fd,socket,core_util)
        self.remote_pid = 1

    def remove(self):
        core_util = self.core_util
        connections_pool = core_util.available_forward_connections[self.remote_pid]
        forward_connection=core_util.fd_2_connection_table[self.fd]
        connections_pool.remove(forward_connection)
        if len(connections_pool)==0:
            del core_util.available_forward_connections[self.remote_pid]

    def recv(self):
        previous_status=self.status
        Connection.recv(self)
        current_status=self.status
        if current_status==ConnectionStatus.dead and previous_status==ConnectionStatus.ready:
            self.core_util.log.debug("remove ready forward connection")
            self.remove()

    def close(self):
        Connection.close(self)
        core_util = self.core_util
        fd = self.fd
        self.core_util.log.debug("close forward connection {}".format(fd))
        forward_2_service_table = core_util.forward_2_service_table
        if fd in forward_2_service_table:
            connection_in_pair = forward_2_service_table[fd]
            connection_in_pair.status = ConnectionStatus.dead
            del forward_2_service_table[fd]

class ServerForwardConnection(ForwardConnection):
    def __init__(self,fd,socket,core_util):
        ForwardConnection.__init__(self,fd,socket,core_util)

    def schedule_when_epoll_in(self):
        ForwardConnection.recv(self)
        status = self.status
        if status == ConnectionStatus.established:
            self.core_util.log.debug("recv_password_authentic")
            self.recv_password_authentic()
        elif status==ConnectionStatus.working:
            self.core_util.log.debug("server_forward_connection handle_downstream_request")
            self.handle_downstream_request()

    def schedule_when_epoll_out(self):
        status = self.status
        if status == ConnectionStatus.established:
            self.core_util.log.debug("server_forward_connection send_password_response")
            self.send_password_response()
        elif status==ConnectionStatus.working:
            self.core_util.log.debug("server_forward_connection handle_upstream_request")
            self.handle_upstream_request()

    def handle_downstream_request(self):
        core_util = self.core_util
        fd = self.fd
        epoll = core_util.epoll
        forward_2_service_table=core_util.forward_2_service_table
        connection_in_pair=forward_2_service_table[fd]
        # core_util.log.debug(self.read_buffer)
        connection_in_pair.write_buffer+=self.read_buffer
        self.read_buffer=""
        epoll.modify(connection_in_pair.fd, select.EPOLLOUT)

    def handle_upstream_request(self):
        self.send()
        if len(self.write_buffer)==0:
            self.core_util.epoll.modify(self.fd, select.EPOLLIN)

    def recv_password_authentic(self):
        log=self.core_util.log
        if len(self.read_buffer)>=36:
            try:
                remote_pid, password= struct.unpack("i33p", self.read_buffer)
                real_password=self.core_util.data_config["password"]
                log.debug(password)
                log.debug(padding(real_password))
                if password == padding(real_password):
                    self.remote_pid = remote_pid
                    core_util = self.core_util
                    epoll = core_util.epoll
                    epoll.modify(self.fd, select.EPOLLOUT)
                    self.read_buffer=""
                    log.debug("confirm password succeed, remote pid:{}".format(remote_pid))
                else:
                    self.status = ConnectionStatus.dead
                    log.warn("wrong password")
            except Exception as e:
                log.warn("fail to parse password_authentic")
                log.debug(e)
                self.status = ConnectionStatus.dead

    def send_password_response(self):
        if len(self.write_buffer)==0:
            pid = os.getpid()
            pack = struct.pack("?i", True, pid)
            self.write_buffer=pack
        self.send()
        if len(self.write_buffer)==0:
            remote_pid=self.remote_pid
            available_forward_connections = self.core_util.available_forward_connections
            available_forward_connections.setdefault(remote_pid, [])
            available_forward_connections[remote_pid].append(self)
            self.status = ConnectionStatus.ready
            self.core_util.epoll.modify(self.fd, select.EPOLLIN)

class ClientForwardConnection(ForwardConnection):
    def __init__(self,fd,socket,core_util):
        ForwardConnection.__init__(self,fd,socket,core_util)

    def schedule_when_epoll_in(self):
        self.recv()
        status = self.status
        if status == ConnectionStatus.established:
            self.core_util.log.debug("recv_password_response")
            self.recv_password_response()
        elif status == ConnectionStatus.ready:
            self.core_util.log.debug("client_forward_connection spawn_client_service_connection")
            self.spawn_client_service_connection()
            self.request_upstream()
        elif status == ConnectionStatus.working:
            self.core_util.log.debug("client_forward_connection request_upstream")
            self.request_upstream()

    def schedule_when_epoll_out(self):
        status=self.status
        if status==ConnectionStatus.established:
            self.core_util.log.debug("send_password_authentic")
            self.send_password_authentic()
        elif status == ConnectionStatus.working:
            self.core_util.log.debug("client_forward_connection handle_downstream_request")
            self.handle_downstream_request()

    def handle_downstream_request(self):
        self.send()
        if len(self.write_buffer)==0:
            self.core_util.epoll.modify(self.fd, select.EPOLLIN)

    def spawn_client_service_connection(self):
        core_util=self.core_util
        data_config = core_util.data_config
        client_host = data_config["client_host"]
        service_port = data_config["client_service_port"]
        service_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        service_address = (client_host, service_port)
        try:
            service_socket.connect(service_address)
            service_socket.setblocking(False)
        except socket.error as e:
            log=core_util.log
            log.warn("connent to client server error, abort")
            log.error(e)
            self.status=ConnectionStatus.dead
            return
        fd = service_socket.fileno()
        core_util.epoll.register(fd, select.EPOLLOUT)
        core_util.fd_2_socket_table[fd] = service_socket
        client_service_connection = ClientServiceConnection(fd, service_socket, core_util)
        core_util.fd_2_connection_table[fd] = client_service_connection
        core_util.forward_2_service_table[self.fd]=client_service_connection
        core_util.service_2_forward_table[fd]=self
        self.status=ConnectionStatus.working
        client_service_connection.status=ConnectionStatus.working

    def request_upstream(self):
        core_util = self.core_util
        fd = self.fd
        epoll = core_util.epoll
        forward_2_service_table=core_util.forward_2_service_table
        connection_in_pair=forward_2_service_table[fd]
        connection_in_pair.write_buffer+=self.read_buffer
        self.read_buffer=""
        epoll.modify(connection_in_pair.fd, select.EPOLLOUT)

    def send_password_authentic(self):
        if len(self.write_buffer)==0:
            pid = os.getpid()
            pack = struct.pack("i33p", pid,padding(self.core_util.data_config["password"]))
            self.write_buffer=pack
        self.send()
        if len(self.write_buffer)==0:
            core_util = self.core_util
            epoll=core_util.epoll
            epoll.modify(self.fd, select.EPOLLIN)

    def recv_password_response(self):
        log = self.core_util.log
        if len(self.read_buffer)>=8:
            try:
                is_success, remote_pid= struct.unpack("?i", self.read_buffer)
                if is_success:
                    self.remote_pid=remote_pid
                    self.status=ConnectionStatus.ready
                    available_forward_connections=self.core_util.available_forward_connections
                    available_forward_connections.setdefault(remote_pid,[])
                    available_forward_connections[remote_pid].append(self)
                    self.read_buffer=""
                    log.debug("confirm password succeed, remote pid:{}".format(remote_pid))
                else:
                    self.status = ConnectionStatus.dead
                    log.warn("fail to confirm password")
            except:
                self.status = ConnectionStatus.dead
                log.warn("fail to parse password_response")

class ServiceConnection(Connection):
    def __init__(self,fd,socket,core_util):
        Connection.__init__(self,fd,socket,core_util)
        self.status=ConnectionStatus.ready

    def request_upstream(self):
        core_util = self.core_util
        fd = self.fd
        epoll = core_util.epoll
        service_2_forward_table=core_util.service_2_forward_table
        connection_in_pair=service_2_forward_table[fd]
        connection_in_pair.write_buffer=self.read_buffer
        self.read_buffer=""
        epoll.modify(connection_in_pair.fd, select.EPOLLOUT)

    def close(self):
        Connection.close(self)
        core_util = self.core_util
        fd = self.fd
        self.core_util.log.debug("close service connection {}".format(fd))
        service_2_forward_table = core_util.forward_2_service_table
        if fd in service_2_forward_table:
            connection_in_pair = service_2_forward_table[fd]
            connection_in_pair.status = ConnectionStatus.dead
            del service_2_forward_table[fd]

class ServerServiceConnection(ServiceConnection):
    def __init__(self,fd,socket,core_util):
        ServiceConnection.__init__(self,fd,socket,core_util)

    def schedule_when_epoll_in(self):
        self.recv()
        core_util=self.core_util
        core_util.log.debug("server_service_connection receive request")
        status = self.status
        core_util.log.debug(status)
        if status==ConnectionStatus.ready:
            core_util.log.debug("need schedule")
        elif status==ConnectionStatus.working:
            core_util.log.debug("server_service_connection request_upstream")
            self.request_upstream()

    def schedule_when_epoll_out(self):
        self.core_util.log.debug("server_service_connection response")
        status=self.status
        if status==ConnectionStatus.working:
            self.core_util.log.debug("server_service_connection working")
            self.send()
            if len(self.write_buffer) == 0:
                core_util = self.core_util
                epoll = core_util.epoll
                epoll.modify(self.fd, select.EPOLLIN)

class ClientServiceConnection(ServiceConnection):
    def __init__(self,fd,socket,core_util):
        ServiceConnection.__init__(self,fd,socket,core_util)

    def request_downstream(self):
        core_util = self.core_util
        fd = self.fd
        epoll = core_util.epoll
        service_2_forward_table = core_util.service_2_forward_table
        connection_in_pair = service_2_forward_table[fd]
        connection_in_pair.write_buffer += self.read_buffer
        self.read_buffer = ""
        epoll.modify(connection_in_pair.fd, select.EPOLLOUT)

    def schedule_when_epoll_in(self):
        self.recv()
        self.core_util.log.debug("client_service_connection request_upstream")
        self.request_upstream()

    def schedule_when_epoll_out(self):
        self.send()
        self.core_util.log.debug("client_service_connection request_local_service")
        if len(self.write_buffer)==0:
            core_util = self.core_util
            epoll=core_util.epoll
            epoll.modify(self.fd, select.EPOLLIN)