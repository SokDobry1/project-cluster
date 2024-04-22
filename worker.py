import socket
from threading import Thread, Lock
import time

import json
import random
from math import *

def read_graph_from_file(filename):
    with open(filename, 'r') as file:
        data = json.load(file)
        return data


def graph_merger(main_graph:dict, graph1:dict, graph2:dict):
    return {**graph1, **graph2, **main_graph} 

associative_graph = {}                
i_start = 0
j_end = inf

def calculate_matrix(graph):
    global associative_graph, i_start, j_end
    n = len(graph)
    
    values = list(associative_graph.values())
    i = i_start
    for value in list(graph.keys()):
        if value not in values:
            associative_graph[i] = value
            i += 1

    for k in range(n):
        for i in range(i_start, n):
            flag = False
            for j in range(min(n, j_end)):
                if (i == j):
                    flag = True
                    break
                gi, gj, gk = associative_graph[i], associative_graph[j], associative_graph[k]
                answ = min(graph[gi].get(gj, inf), graph[gi].get(gk, inf) + graph[gk].get(gj, inf))
                if answ != inf: 
                    graph[gi][gj] = answ
                    graph[gj][gi] = answ
            if flag: continue
    i_start = n
    j_end = n+1
    
    return graph



lock = Lock()
host_addr = ("0.0.0.0", 8888)
manager_port = 8888
manager_addr = ()

graph = {}
full_graph = {}


def socket_sender(addr:tuple, message:dict): # addr = (ip, port)
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect(addr)
        s.sendall(json.dumps(message).encode())

def handle_client(conn, addr):
    global lock, host_addr, full_graph, graph, manager_addr, manager_port

    recived = ''
    try:
        while True:
            data = conn.recv(1024)
            if not data:
                break
            recived += data.decode()    # {"type": "requires a solution", "data": {}} ; {"type": "graph", "data": {"graph": {"x": {"y": w}}}, ... }; {"type": "request graph", "data":{"addr": (ip, port)}}
    finally:                            # {"type": "starter pack", "data": "graph": {}, "task": {}}
        conn.close()

    recived = json.loads(recived)
    with lock:
        global host_addr
        if recived["type"] == "starter pack":
            print("Получил стартвый пакет данных")
            global associative_graph, i_start, j_end
            associative_graph = {}; i_start = 0 ; j_end = inf

            manager_addr = (addr[0], manager_port)
            full_graph = recived["data"]["graph"]
            graph = calculate_matrix(recived["data"]["task"])
            print("Первые вычисления окончены, отправляю запрос на дальнейшие действия")
            socket_sender(manager_addr, {"type": "requires a solution", "data":{}, "addr": (host_addr)})

        if recived["type"] == "request graph":
            print("Пришёл запрос на граф, отправляю", recived["data"]["addr"])
            socket_sender(tuple(recived["data"]["addr"]), {"type": "graph", "data": {"graph":graph}})
            
        if recived["type"] == "graph":
            print("Пришёл запрос на дополнение графа")
            graph = graph_merger(full_graph, graph, recived["data"]["graph"])
            graph = calculate_matrix(graph)
            print("Дополнение графа окончено, отправляю запрос на дальнейшие действия")
            socket_sender(manager_addr, {"type": "requires a solution", "data":{}, "addr": (host_addr)})








    

def start_server():
    global host_addr
    host, port = host_addr
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((host, port))
        s.listen()
        print(f'Server started on {host}:{port}')
        while True:
            conn, addr = s.accept()
            Thread(target=handle_client, args=(conn, addr)).start()

if __name__ == '__main__':
    start_server()