import pickle
import time
import socket
from .functions import *
import uuid, threading


"""
"10000"
"10001"
"10010"
"10011"
"10100"
"10101"
"10110"
"""

class Server:
    def __init__(self, ip="127.0.0.1", port=8000, connections=5):
        
        self.td = []
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_address = (ip, port)
        
        try:
            self.sock.bind(self.server_address)
            self.sock.listen(connections)
        except OSError as e:
            print("Type:", type(e), "\nException:", e)

    def start(self, target, arg):
        try:
            while True:
                conn, addr = self.sock.accept()
                # print("Accepted connection from:", addr)
                self.td.append(threading.Thread(target=target, args=(conn, addr, arg)))
                self.td[-1].start()

                if len(self.td) > 50:
                    self.td.pop(0)
        except Exception as e:
            print("Exception", e)
        finally:
            for t in self.td:
                t.join()
        
            self.sock.close()


class Code():
    def __init__(self, module_name, module_alias=""):
        self.module_name = module_name
        self.module_alias = module_alias if module_alias else module_name
        
        f = open(module_name + ".py", "r")
        self.data = f.read()
        f.close()

        for attr in getattrs(module_name, self.module_alias):
            self.__dict__[attr[1]] = attr


class File():
    def __init__(self, file_name):
        self.file_name = file_name
        
        f = open(file_name, "r")
        self.data = f.read()
        f.close()


class Client():
    def __init__(self, ID):
        self.code = pickle.dumps([])
        self.files = pickle.dumps([])
        self.id = ID
        self.address = ("127.0.0.1", 3000)

    def map(self):
        pass

    def reduce(self):
        pass

    def filter(self):
        pass


class DataNode():
    def __init__(self):
        self.server = Server()
        self.score = self.benchmark()
        self.clients = {}
        self.id = str(uuid.uuid1())
        self.data = {
            "id": self.id,
            "auth": "Auth",
            "options": {
                "encryption": False,
                "compression": False,
            },
        }
        self.function_map = {
            "00000": self.client_register,
            "00001": self.client_push_code,
            "00010": self.client_push_files,
            "00011": self.client_map,
            "00100": self.client_filter,
            "00101": self.client_reduce,
        }

    def start(self):
        while True:
            # Accept connections
            # Handle requests
            # Requests can be from namenode
            self.server.start(handle, (self, ))

    def benchmark(self):
        return 100

    def register(self, addr):
        self.data["type"] = "10000"
        self.data["data"] = {"server_address": self.server.server_address}

        response = send(self.data, addr)
        print(response)

    def get_client(self, ID):
        return self.clients[ID]

    def client_register(self, ID, data):
        print(data)
        self.clients[ID] = (Client(ID))
        print("client Registered")

    def client_push_code(self, ID, data):
        print(data)
        client = self.get_client(ID)
        client.code = data

    def client_push_files(self, ID, data):
        print(data)
        client = self.get_client(ID)
        client.files = data

    def client_map(self, ID, data):
        print(data)
        pass

    def client_filter(self, ID, data):
        print(data)
        pass

    def client_reduce(self, ID, data):
        print(data)
        pass

    def client_close(self, ID, data):
        print(data)
        pass


if __name__ == "__main__":
    try:
        main = DataNode()
        main.register(("127.0.0.1", 8000))
        print("registered")
        main.start()
    except KeyboardInterrupt:
        main.server.stop()
        print("Goodbye....")