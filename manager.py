import socket
from threading import Thread, Lock
import time

from math import *
import json
import random

def read_graph_from_file(filename):
    with open(filename, 'r') as file:
        data = json.load(file)
        return data

def generate_random_connected_graph(nodes, edge_probability):
    """
    Генерирует случайный связный граф с заданным количеством узлов и вероятностью образования ребра.
    """
    graph = {i: {} for i in range(nodes)}
    # Добавляем ребра с заданной вероятностью
    for i in range(nodes):
        print(i)
        for j in range(nodes):
            if i == j: break
            if (edge_probability > random.randint(0, 100)):
                graph[i][j] = 1
                graph[j][i] = 1
    return graph

# Генерируем граф
#random_graph = read_graph_from_file("output_file.json")
random_graph = generate_random_connected_graph(1000, 50)  # Пример для __ узлов и вероятности образования ребра __%

def divide_graph_into_equal_subgraphs(graph, n):
    vertices = list(graph.keys())
    num_vertices = len(vertices)
    vertices_per_subgraph = num_vertices // n

    subgraphs = []
    for i in range(n):
        print(i)
        start_idx = i * vertices_per_subgraph
        end_idx = (i + 1) * vertices_per_subgraph if i < n - 1 else num_vertices
        subgraph_vertices = vertices[start_idx:end_idx]

        subgraph = {}
        for vertex in subgraph_vertices:
            subgraph[vertex] = {k: v for k, v in graph[vertex].items() if k in subgraph_vertices}

        subgraphs.append(subgraph)

    return subgraphs

workers = [#("192.168.14.36", 8887), ("192.168.14.36", 8888), 
           #("192.168.14.180", 8888), ("192.168.14.180", 8887), 
           ("192.168.14.35", 8887), ("192.168.14.35", 8888),
        ] 
workers_port = 8887
nodes = len(workers)



graphs = divide_graph_into_equal_subgraphs(random_graph, nodes)

print("Начинаю запись в файл")
with open('output_file.json', 'w') as file:
    # Выводим граф в требуемом формате
    json.dump({str(node): {str(neighbor): random_graph[node][neighbor] for neighbor in random_graph[node]} for node in random_graph}, file)
print("Закончил запись в файл")
    


lock = Lock()
buffer = []
finished = False
host_addr = ("192.168.14.38", 8888)


def socket_sender(addr:tuple, message:dict): # addr = (ip, port)
    print("Отправляю данные", addr, message["type"])
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect(addr)
        s.sendall(json.dumps(message).encode())

def handle_client(conn, addr):
    global lock, buffer, nodes, finished, host_addr, workers_port

    recived = ''
    try:
        while True:
            data = conn.recv(1024)
            if not data:
                break
            recived += data.decode()    # {"type": "requires a solution", "data": {}} ; {"type": "graph", "data": {"graph": {"x": {"y": w}}}, ... }; {"type": "request graph", "data":{"addr": (ip, port)}}
    finally:                            # {"type": "starter pack", "data": {"graph": {}, "task": {}}}
        conn.close()
    recived = json.loads(recived)


    with lock:
        print(buffer, nodes)
        if recived["type"] == "graph":
            print("Вычисления окончены")
            with open("result.json", 'w') as file:
                # Выводим граф в требуемом формате
                json.dump(recived["data"]["graph"], file)
            
            open("time.txt", 'w').write(str(time.time() - t) + " sec")
            return

        if recived["type"] == "requires a solution":
            print("Одна из нод завершила работу, ожидаю пару...")
            #if addr[0] not in buffer:
            buffer.append(addr[0])

        if len(buffer) == 1 and nodes <= 1:
            print("Близимся к завершению")
            socket_sender((buffer[0], workers_port), {"type": "request graph", "data":{"addr": host_addr}})
            buffer = []
            return
        
        if len(buffer) >= 2:
            print("Отправляю данные на объединение")
            nodes -= 1
            socket_sender((buffer[0], workers_port), {"type": "request graph", "data":{"addr": (buffer[1], workers_port)}})
            buffer = buffer[2:]
            return



    

def start_server():
    global finished, host_addr
    host, port = host_addr
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((host, port))
        s.listen()
        print(f'Server started on {host}:{port}')
        while True:
            conn, addr = s.accept()
            Thread(target=handle_client, args=(conn, addr)).start()

if __name__ == '__main__':
    t = time.time()
    for i in range(nodes): Thread(target=socket_sender, args=(workers[i], {"type": "starter pack", "data": {"task": graphs[i], "graph": random_graph}})).start()
    start_server()