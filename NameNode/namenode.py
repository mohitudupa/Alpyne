import socket
import threading
import pickle
import time
import math
import uuid
from .functions import *
from .containers import *
name_node = None

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
        self.server_address = server_address
        self.assigned_jobs = 0

    def add_jobs(self, new_jobs_assigned):
        self.assigned_jobs += new_jobs_assigned

    def remove_jobs(self, remove_jobs_assigned):
        self.assigned_jobs -= remove_jobs_assigned


class Client:
    def __init__(self, client_id, username, password, input_container, output_container, operation):
        self.client_id = client_id
        self.state = "init"
        self.username = username
        self.password = password
        self.input_container = input_container
        self.output_container = output_container
        self.operation = operation
        self.jobs = {}
        self.containers = Containers(db_host, db_port, username, password, user_id)
        self.containers.open(self.input_container)
        self.input_file_names = self.containers.list_files()
        self.njobs = len(self.input_file_names)
        for input_file_name in self.input_file_names:
            self.jobs[input_file_name] = None

    def assign_workers(self, data_nodes):
        #mohit code
        for job_id in range(self.njobs):
            pass
        
        self.containers.open(self.input_container)
        input_files = self.containers.list_files

        data_node_stats = name_node.get_data_nodes()
        sum_benchmark = 0
        sum_load = 0
        #data_node_stats = {data_node_id : [benchmark,load,...]}
        for data_node_id,data_node_stat in data_node_stats.items():
            sum_benchmark += data_node_stat[0]
            sum_load += data_node_stat[1]
        #share = {data_node_id : number_of_files_to_be_sent} 
        share = {}
        for data_node_id,data_node_stat in data_node_stats.items():
            ratio = data_node_stat[0]/sum_benchmark
            share[data_node_id] = math.ceil(ratio * self.njobs)
        print(share)
        offset = 0
        for data_node_id in data_node_stats:
            print(offset)
            for input_file in self.input_file_names[offset:offset + share[data_node_id]]:
                self.jobs[input_file] = data_node_id
            offset += share[data_node_id]
        print(self.jobs)

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
            sum_score = 0
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
    def get_data_nodes(self):
        #returns the list of data_nodes and its stats
        data_nodes = {}
        for data_node_id, data_node in self.data_nodes.items():
            data_nodes[data_node_id] = data_node.get_stats()
        return data_nodes

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
        global name_node
        name_node = NameNode()
        server = Server()
        server.start(handle, name_node)
    except KeyboardInterrupt:
        print("Goodbye....")