# -*- coding: utf-8 -*-
from __future__ import print_function
import json
import time
import datetime
import logging
import os
import socket
import select
from multiprocessing import Process
from util import CoreUtil,ClientForwardConnection,ConnectionStatus

max_connection=1024
epoll_wait_timeout = 10
path_current = os.path.split(os.path.realpath(__file__))[0]
log=None

def get_date():
    return datetime.datetime.now().strftime('%Y-%m-%d')

def init():
    global log
    path_logs=os.path.join(path_current,"logs")
    if not os.path.exists(path_logs):
        os.mkdir(path_logs)
    log = logging.getLogger('{}'.format(get_date()))
    log.setLevel(logging.DEBUG)
    fmt = logging.Formatter(fmt="%(asctime)s[]%(message)s", datefmt='%H:%M:%S')

    terminal_handler = logging.StreamHandler()
    terminal_handler.setLevel(logging.ERROR)
    terminal_handler.setFormatter(fmt)

    path_log=os.path.join(path_logs,'client_{}.log'.format(get_date()))
    file_handler = logging.FileHandler(path_log, mode="w", encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(fmt)

    log.addHandler(terminal_handler)
    log.addHandler(file_handler)

def worker(data_config=None):
    server_host=data_config["server_host"]
    forward_port=data_config["forward_port"]
    epoll = select.epoll()
    fd_2_socket_table = {}
    core_util = CoreUtil(data_config)
    core_util.fd_2_socket_table = fd_2_socket_table
    core_util.log=log
    core_util.epoll=epoll

    while True:
        need_spawn_new_connection=False
        remote_forward_connections=core_util.available_forward_connections
        count_remote_forward_connections=len(remote_forward_connections.keys())
        log.debug("available_forward_connection_number:{}".format(count_remote_forward_connections))
        if count_remote_forward_connections<data_config["num_process"]:
            need_spawn_new_connection=True
        if need_spawn_new_connection:
            log.debug("spawn new connection:{}".format(count_remote_forward_connections))
            forward_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            fordward_address = (server_host, forward_port)
            forward_socket.connect(fordward_address)
            forward_socket.setblocking(False)
            fd = forward_socket.fileno()
            epoll.register(fd, select.EPOLLOUT)
            fd_2_socket_table[fd] = forward_socket
            client_forward_connection = ClientForwardConnection(fd, forward_socket, core_util)
            core_util.fd_2_connection_table[fd] = client_forward_connection
        log.info("{} wait......".format(os.getpid()))
        events = epoll.poll(epoll_wait_timeout)
        if not events:
            log.info("epoll timeout......")
            # current_remote_forward_connections=[]
            # for remote_ip in remote_forward_connections:
            #     current_remote_forward_connections+=remote_forward_connections[remote_ip]
            # current_remote_forward_connections=[connection.status for connection in current_remote_forward_connections]
            # log.debug(current_remote_forward_connections)
            continue
        for fd, event in events:
            current_connection = core_util.fd_2_connection_table[fd]
            if event & select.EPOLLIN:
                current_connection.schedule_when_epoll_in()
            elif event & select.EPOLLOUT:
                current_connection.schedule_when_epoll_out()
            else:
                current_connection.status=ConnectionStatus.dead
        for current_connection in core_util.fd_2_connection_table.values():
            if current_connection.status == ConnectionStatus.dead:
                current_connection.close()
    epoll.close()

def main():
    init()
    file_config=os.path.join(path_current,"config.json")
    if os.path.exists(file_config):
        data_config=json.loads(open(file_config).read().encode("utf-8"))
    else:
        log.warn("no config file found, use default")
        data_config={
          "server_host": "172.17.231.150",
          "forward_port": 8000,
          "server_service_port": 9000,
          "client_host": "localhost",
          "client_service_port": 27017,
          "num_process": 1,
          "password": "pfrp"
        }
    process_num=data_config["num_process"]
    processes=[]
    for i in range(process_num):
        p = Process(target=worker, args=(data_config,))
        processes.append(p)
    for p in processes:
        p.start()
    for p in processes:
        p.join()

if __name__=="__main__":
    main()