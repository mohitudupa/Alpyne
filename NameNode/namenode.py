import socket
import threading
import pickle
import time
import uuid
from .functions import *
import containers

db_host, db_port = "localhost", 27017
users = {
    "80cf72a4083211e9aaacf8cab814c762": ("mohit", "password for fs")
}

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


class DataNode:
    def __init__(self, data_node_id, server_address):
        self.id = data_node_id
        self.score = None
        self.server_address = server_address;
        self.assigned_jobs = 0

    def add_jobs(self, new_jobs_assigned):
        self.assigned_jobs += new_jobs_assigned

    def remove_jobs(self, remove_jobs_assigned):
        self.assigned_jobs -= remove_jobs_assigned


class Client:
    def __init__(self, user_id, username, password):
        self.id = user_id
        self.state = "stop"
        self.username = username
        self.password = password
        self.containers = containers.Containers(db_host, db_port, username, password, user_id)

    def assign_job(self, input_container, output_container, operation):
        self.input_container = input_container
        self.output_container = output_container
        self.operation = operation

    def assign_workers(self, data_nodes):
        self.containers.open(self.input_container)
        
        size = self.containers.len_files()
        pool_values = []
        for data_node in data_nodes:
            pool_values.append(data_node.score / (data_node.assigned_jobs + 1))

        print(pool_values)
        pool_total = sum(pool_values)
        



class Job:
    def __init__(self, user_id, job_id, input_container, output_container, operation):
        self.user_id = user_id
        self.job_id = job_id
        self.input_container = input_container
        self.output_container = output_container
        self.operation = operation
        self.jobs = {}
        self.state = "init"

    def assign_workers(self, data_nodes, client):
        client.containers.open(self.input_container)
        
        client.self.input_container = self.input_container
        self.output_container = self.output_container



        # Get worker list
        for data_node in data_nodes:
            pass
        self.state = "ready"
        pass

    def start_workers(self):
        # Send job to the worker
        self.state = "start"
        pass

    def track_workers(self):
        # Track workers who have finished their  jobs
        # If all jobs are finished mark job as finished
        pass

    def realloc_worker(self, worker_id):
        # Realloc stalled workers
        pass


class NameNode:
    def __init__(self):
        self.clients = {}
        self.data_nodes = {}
        self.function_map = {
            10: self.client_map,
            11: self.client_filter,
            12: self.client_reduce,
            1 : self.client_result,

            90: self.data_node_register,
            91: self.data_node_heartbeat,
            92: self.data_node_report,
            80: self.data_node_map_result,
            81: self.data_node_reduce_result,
            82: self.data_node_filter_result,
            99: self.data_node_close,
        }

    # Event handlers
    # Client handlers

    def client_map(self, user_id, data):
        print("map")
        print(data)

        # Check the user
        if user_id not in users:
            return {"status": "Invalid user ID"}
        # Get client listing
        client = self.clients.get(user_id, Client(user_id, *users[user_id]))

        input_container = data["input_container"]
        output_container = data["output_container"]

        # Do type checking and validation
        try:
            client.containers.open(input_container)
            if not client.containers.len_files():
                raise ValueError("Invalid input length")
        except ValueError as ve:
            return {"status": str(ve)}
        except Exception as e:
            return {"status": "General error" + str(e)}

        # Create job listing
        job_id = uuid.uuid1().hex
        job = Job(user_id, job_id, input_container, output_container, "map")

        self.clients[user_id].jobs[job_id] = job

        # Get available data nodes
        job.assign_workers(self.data_nodes, client)

        # Distribute and send jobs to datanodes
        job.start_workers()

        return {"status": "Data queued for map", "job_id": job_id}

    def client_filter(self, user_id, data):
        print("filter")
        print(data)
        input_container = data["input_container"]
        output_container = data["output_container"]

        # Check the user
        if user_id not in users:
            return {"status": "Invalid user ID"}
        # Get client listing
        client = self.clients.get(user_id, Client(user_id, *users[user_id]))

        # Do type checking and validation
        try:
            client.containers.open(input_container)
            if not client.containers.len_files():
                raise ValueError("Invalid input length")
        except ValueError as ve:
            return {"status": str(ve)}
        except Exception as e:
            return {"status": "General error"}

        # Create job listing
        job_id = uuid.uuid1().hex
        job = Job(user_id, job_id, input_container, output_container, "filter")

        self.clients[user_id].jobs[job_id] = job

        # Get available data nodes
        job.assign_workers(self.data_nodes, client)

        # Distribute and send jobs to datanodes
        job.start_workers()

        return {"status": "Data queued for map", "job_id": job_id}

    def client_reduce(self, user_id, data):
        print("reduce")
        print(data)
        input_container = data["input_container"]
        output_container = data["output_container"]

        # Check the user
        if user_id not in users:
            return {"status": "Invalid user ID"}
        # Get client listing
        client = self.clients.get(user_id, Client(user_id, *users[user_id]))

        # Do type checking and validation
        try:
            client.containers.open(input_container)
            if not client.containers.len_files():
                raise ValueError("Invalid input length")
        except ValueError as ve:
            return {"status": str(ve)}
        except Exception as e:
            return {"status": "General error " + str(e)}

        # Create job listing
        job_id = uuid.uuid1().hex
        job = Job(user_id, job_id, input_container, output_container, "reduce")

        self.clients[user_id].jobs[job_id] = job

        # Get available data nodes
        job.assign_workers(self.data_nodes, client)

        # Distribute and send jobs to datanodes
        job.start_workers()

        return {"status": "Data queued for map", "job_id": job_id}

    def client_result(self, ID, data):
        print("result")
        # Communicate with the data nodes
        
        return {"status": "Done"}


    # Section handle data_nodes
    def data_node_register(self, ID, data):
        self.data_nodes[ID] = DataNode(ID, data["server_address"])
        print(self.data_nodes)
        return {"data": "registered"}

    def data_node_heartbeat(self, ID, data):
        pass

    def data_node_report(self, ID, data):
        pass

    def data_node_map_result(self, ID, data):
        pass

    def data_node_reduce_result(self, ID, data):
        pass

    def data_node_filter_result(self, ID, data):
        pass

    def data_node_close(self, id, data):
        pass


if __name__ == "__main__":
    try:
        name_node = NameNode()
        server = Server()
        server.start(handle, name_node)
    except KeyboardInterrupt:
        print("Goodbye....")