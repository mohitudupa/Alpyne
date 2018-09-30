import pickle
import time
import ALP


def getattrs(module_name, module_alias):
    x = __import__(module_name)
    return[(module_alias, i) for i in dir(x) if callable(getattr(x, i))]


def handle_request(conn, request):
    conn.send(str(request.data).encode("utf-8"))
    return conn.recv(4096)


def send_data(request):
    client = ALP.Client()
    return client.start(handle_request, (request, ), *request.address)

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
        self.address = ("127.0.0.1", 10000)

        # Registering the request with the namenode
        self.data["type"] = "0000"
        send_data(self)


    def push_code(self, code):
        if not isinstance(code, list):
            code = [code]

        self.code = pickle.dumps(pickle.loads(self.code) + code)

        # Code to send the code pickle to namenode
        self.data["type"] = "0001"
        self.data["data"] = {"code": self.code}
        send_data(self)

    def push_files(self, files):
        if not isinstance(files, list):
            files = [files]

        self.files = pickle.dumps(pickle.loads(self.files) + files)

        # Code to send the files pickle to namenode
        self.data["type"] = "0010"
        self.data["data"] = {"files": self.files}

        send_data(self)

    def flush_code(self):
        self.code = pickle.dumps([])

    def flush_files(self):
        self.code = pickle.dumps([])

    def map(self, function, data):
        # Code to send map instruction to name node
        self.data["type"] = "0011"
        self.data["data"] = {
            "function": function,
            "data": data,
        }
        send_data(self)

    def filter(self):
        # Code to send map instruction to name node
        self.data["type"] = "0100"
        self.data["data"] = {
            "function": function,
            "data": data,
        }
        send_data(self)

    def reduce(self):
        # Code to send map instruction to name node
        self.data["type"] = "0101"
        self.data["data"] = {
            "function": function,
            "data": data,
        }
        send_data(self)

    def get_async_result(self):
        # Code to ask result to the name node
        self.data["type"] = "0110"
        self.data["data"] = {}
        return send_data(self)

    def get_result(self, maximum=100):
        delay = 1
        while(1):
            result = self.get_async_result()
            if result:
                return result
            delay = min(maximum, delay * 2)
            time.sleep(delay)

