import requests
import math


class Task:
    def __init__(self, dbs_h, dbs_p, username, password):
        self.session = requests.session()
        self.DBS_H = dbs_h
        self.DBS_P = dbs_p

        self.logged_in = False

        self.username = username
        self.password = password

        self.compute_nodes = []

        self.share = {}

    def login(self):
        res = self.session.post(f'http://{self.DBS_H}:{self.DBS_P}/user/login/', data={
            'username': self.username,
            'password': self.password
        })

        if res.status_code != 200:
            self.logged_in = False
            return False
        
        self.logged_in = True
        return True
    
    def logout(self):
        self.session.get(f'http://{self.DBS_H}:{self.DBS_P}/user/logout/')
    
    def get_compute_nodes(self):
        if self.logged_in:
            self.compute_nodes = self.session.get('http://{self.DBS_H}:{self.DBS_P}/compute/nodes/')

    def distribute(self, files: list) -> None:
        """
        This function distributes the task among available compute nodes
        :param files: A list of files
        :return: None
        """
        self.get_compute_nodes()
        sum_benchmark = 0

        n_jobs = len(files)

        for compute_node in self.compute_nodes:
            sum_benchmark += compute_node["score"]

        for compute_node in self.compute_nodes:
            ratio = compute_node["score"]/sum_benchmark
            self.share[compute_node['hostname']] = math.ceil(ratio * n_jobs)

        for compute_node, share_count in self.share.items():
            try:
                self.share[compute_node] = [files.pop(0) for _ in range(share_count)]
            except IndexError:
                self.share[compute_node] = files[:]

    def task(self, code: str, inp: str, out: str):
        failed = []
        for compute_node in self.compute_nodes:
            hostname = compute_node['hostname']
            share_count = self.share[hostname]
            if not share_count:
                continue
            
            data = {}
            data['code']: code
            data['args'] = [[inp, out, file] for file in self.share[hostname]]
            data['kwargs'] = [{} for _ in self.share[hostname]]
        
        return failed


t = None


def main():
    global t
    t = Task('', '', '', '')
    t.distribute(list(range(10)))
    print(t.task('code.py', 'input', 'output'))


main()

"""
[
    {
        'compute_node_id': uuid,
        'hostname': '127.0.0.1:80',
        'score': 231231,
        'description': 'some desc'
    },
    {
        'compute_node_id': uuid,
        'hostname': '127.0.0.1:81',
        'score': 231231,
        'description': 'some desc'
    },
]
"""