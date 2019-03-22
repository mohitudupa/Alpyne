import pickle
import time
from functions import *


class Client():
    def __init__(self, ID, address = ("127.0.0.1", 8000)):
        self.id = ID
        self.data = {
            "id": self.id,
            "auth": "Auth",
        }
        self.address = address

    def map(self, inputs, outputs):
        # Code to send map instruction to name node
        self.data["type"] = 10
        self.data["data"] = {
            "inputs": inputs,
            "outputs": outputs,
        }
        return send(self.data, self.address)

    def filter(self, inputs, outputs):
        # Code to send map instruction to name node
        self.data["type"] = 11
        self.data["data"] = {
            "inputs": inputs,
            "outputs": outputs,
        }
        return send(self.data, self.address)

    def reduce(self, inputs, outputs):
        # Code to send map instruction to name node
        self.data["type"] = 12
        self.data["data"] = {
            "inputs": inputs,
            "outputs": outputs,
        }
        return send(self.data, self.address)

    def result(self):
        # Code to ask result to the name node
        self.data["type"] = 1
        self.data["data"] = {}
        return send(self.data, self.address)

    def result_sync(self, maximum=100):
        delay = 1
        while(1):
            result = self.get_async_result()
            if result:
                return result
            delay = min(maximum, delay + 1)
            time.sleep(delay)
