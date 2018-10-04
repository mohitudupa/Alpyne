import socket
import threading
import pickle
import time
from functions import *


class Server():
    def __init__(self, ip="127.0.0.1", port=8000, connections=5):
        
        self.td = []
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_address = (ip, port)
        
        try:
            self.sock.bind(self.server_address)
            self.sock.listen(connections)
        except OSError as e:
            print("Type:", type(e), "\nException:", e)

    def start(self, target, args):
        
        conn, addr = self.sock.accept()
        # print("Accepted connection from:", addr)
        self.td.append(threading.Thread(target=target, args=(conn, addr, *args)))
        self.td[-1].start()

    def stop(self):
        
        for t in self.td:
            t.join()
        
        self.sock.close()


class DataNode():
    def __init__(self):
        pass


class User():
    def __init__(self):
        pass


class Main():
    def __init__(self):
        self.server = Server()
        pass

    def start(self):
        while True:
            # Accept connections
            # Handle requests
            # Requests can be from datanodes and users
            # Datanode requests can be of type:
            #   Register, Heartbeats, Result
            # User requests can be of type:
            #   Register, Push_code, Push_files, Map, Filter, Reduce, Get_result
            # Namenodes actions can be of type:
            #   Get_status, Create_user, Send_computation
            self.server.start(handle, (self, ))
        pass


if __name__ == "__main__":
    try:
        main = Main()
        main.start()
    except KeyboardInterrupt:
        main.server.stop()
        print("Goodbye....")