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
from util import CoreUtil,ServerServiceConnection,ServerForwardConnection,ConnectionStatus

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
    log.setLevel(logging.ERROR)
    fmt = logging.Formatter(fmt="%(asctime)s[]%(message)s", datefmt='%H:%M:%S')

    terminal_handler = logging.StreamHandler()
    terminal_handler.setLevel(logging.DEBUG)
    terminal_handler.setFormatter(fmt)

    path_log=os.path.join(path_logs,'server_{}.log'.format(get_date()))
    file_handler = logging.FileHandler(path_log, mode="w", encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(fmt)

    log.addHandler(terminal_handler)
    log.addHandler(file_handler)

def worker(data_config=None):
    server_host=data_config["server_host"]
    forward_port=data_config["forward_port"]

    forward_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    forward_socket.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,True)
    forward_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, True)

    fordward_address = (server_host, forward_port)
    forward_socket.bind(fordward_address)
    forward_socket.listen(max_connection)
    forward_socket.setblocking(False)

    service_port = data_config["server_service_port"]
    service_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    service_socket.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,True)
    service_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, True)

    service_address = (server_host, service_port)
    service_socket.bind(service_address)
    service_socket.listen(max_connection)
    service_socket.setblocking(False)

    epoll = select.epoll()
    epoll.register(forward_socket.fileno(), select.EPOLLIN)
    epoll.register(service_socket.fileno(), select.EPOLLIN)

    fd_2_socket_table = {}
    fd_2_socket_table[forward_socket.fileno()]=forward_socket
    fd_2_socket_table[service_socket.fileno()]=service_socket

    core_util = CoreUtil(data_config)
    core_util.fd_2_socket_table = fd_2_socket_table
    core_util.log=log
    core_util.epoll=epoll

    while True:
        log.info("{} wait......,{} connections".format(os.getpid(),len(fd_2_socket_table)))
        events = epoll.poll(epoll_wait_timeout)
        if not events:
            log.info("epoll timeout......")
            continue
        for fd, event in events:
            current_socket = fd_2_socket_table[fd]
            if current_socket == forward_socket:
                income_socket, address = forward_socket.accept()
                log.info("{} new forward connection:{}".format(os.getpid(),address))
                income_socket.setblocking(False)
                fd=income_socket.fileno()
                epoll.register(fd, select.EPOLLIN)
                fd_2_socket_table[fd] = income_socket
                server_forward_connection=ServerForwardConnection(fd,income_socket,core_util)
                core_util.fd_2_connection_table[fd]=server_forward_connection
            elif current_socket == service_socket:
                income_socket, address = service_socket.accept()
                log.info("{} new service connection:{}".format(os.getpid(),address))
                income_socket.setblocking(False)
                fd=income_socket.fileno()
                epoll.register(fd, select.EPOLLIN)
                fd_2_socket_table[fd] = income_socket
                server_service_connection=ServerServiceConnection(fd,income_socket,core_util)
                core_util.fd_2_connection_table[fd]=server_service_connection
                core_util.ready_service_connections.append(server_service_connection)
            else:
                current_connection = core_util.fd_2_connection_table[fd]
                if event & select.EPOLLIN:
                    current_connection.schedule_when_epoll_in()
                elif event & select.EPOLLOUT:
                    current_connection.schedule_when_epoll_out()
                else:
                    current_connection.status=ConnectionStatus.dead
                if current_connection.status == ConnectionStatus.dead:
                    current_connection.close()
            core_util.schedule()
    epoll.close()

def main():
    init()
    file_config=os.path.join(path_current,"config.json")
    if os.path.exists(file_config):
        data_config=json.load(open(file_config))
    else:
        log.warning("no config file found, use default")
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
    # worker(data_config)

if __name__=="__main__":
    main()