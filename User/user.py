import pickle
import time
from functions import *


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


class Request():
    def __init__(self):
        self.code = pickle.dumps([])
        self.files = pickle.dumps([])
        self.id = 1000
        self.data = {
            "id": self.id,
            "auth": "Auth",
            "options": {
                "encryption": False,
                "compression": False,
            },
        }
        self.address = ("127.0.0.1", 8000)

        # Registering the request with the namenode
        self.data["type"] = "00000"
        send(self.data, self.address)

    def push_code(self, code):
        if not isinstance(code, list):
            code = [code]

        self.code = pickle.dumps(pickle.loads(self.code) + code)

        # Code to send the code pickle to namenode
        self.data["type"] = "00001"
        self.data["data"] = {"code": self.code}
        send(self.data, self.address)

    def push_files(self, files):
        if not isinstance(files, list):
            files = [files]

        self.files = pickle.dumps(pickle.loads(self.files) + files)

        # Code to send the files pickle to namenode
        self.data["type"] = "00010"
        self.data["data"] = {"files": self.files}
        send(self.data, self.address)

    def flush_code(self):
        self.code = pickle.dumps([])

    def flush_files(self):
        self.code = pickle.dumps([])

    def map(self, function, data):
        # Code to send map instruction to name node
        self.data["type"] = "00011"
        self.data["data"] = {
            "function": function,
            "data": data,
        }
        send(self.data, self.address)

    def filter(self, function, data):
        # Code to send map instruction to name node
        self.data["type"] = "00100"
        self.data["data"] = {
            "function": function,
            "data": data,
        }
        send(self.data, self.address)

    def reduce(self, function, data):
        # Code to send map instruction to name node
        self.data["type"] = "00101"
        self.data["data"] = {
            "function": function,
            "data": data,
        }
        send(self.data, self.address)

    def get_async_result(self):
        # Code to ask result to the name node
        self.data["type"] = "00110"
        self.data["data"] = {}
        return send(self.data, self.address)

    def get_result(self, maximum=100):
        delay = 1
        while(1):
            result = self.get_async_result()
            if result:
                return result
            delay = min(maximum, delay * 2)
            time.sleep(delay)

