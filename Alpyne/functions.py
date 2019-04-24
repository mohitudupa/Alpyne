from functools import wraps, reduce
import pickle
import requests
import math
import time


def upload_dataset(data, inp, containers):
    pickled_data = [pickle.dumps(x) for x in data]

    containers.open(inp)
    containers.put(pickled_data)

    return containers.list_files()


def download_dataset(out, containers):
    containers.open(out)
    pickled_data = containers.get(containers.list_files())

    data = [pickle.load(x) for x in pickled_data]

    return data


def grid_map(inp: str, out: str, containers):
    def decorator(func):
        @wraps(func)
        def wrapper(file: str):
            containers.open(inp)
            meta = containers.get_one(file)
            arg = pickle.load(meta)

            res = func(arg)

            containers.open(out)
            meta = pickle.dumps(res)
            containers.put_one(meta, file)

        return wrapper
    return decorator


def grid_filter(inp: str, out: str, containers):
    def decorator(func):
        @wraps(func)
        def wrapper(file: str):
            containers.open(inp)
            meta = containers.get_one(file).read()
            arg = pickle.loads(meta)

            res = func(arg)

            if res:
                containers.open(out)
                containers.put_one(meta, file)

        return wrapper
    return decorator


def grid_reduce(inp: str, out: str, containers):
    def decorator(func):
        @wraps(func)
        def wrapper(files: str):
            containers.open(inp)
            meta = containers.get_one(files)
            args = pickle.load(meta)

            res = reduce(func, args)

            containers.open(out)
            meta = pickle.dumps(res)
            containers.put_one(meta)

        return wrapper
    return decorator


def distribute(files, scores):
    files = files[:]
    fraction = len(files) / sum(scores)
    share = []

    for score in scores:
        share_count = math.ceil(score * fraction)
        if share_count <= len(files):
            share.append([files.pop(0) for _ in range(share_count)])
        else:
            share.append(files[:])

    return share


class Task:
    def __init__(self, username, password, host):
        self.username = username
        self.password = password
        self.host = host
        self.session = requests.session()
        self.login()
        self.job_id = None

    def login(self):
        res = self.session.post(url=f'http://{self.host}/user/login/', data={
            'username': self.username,
            'password': self.password
        })

        if res.status_code != 200:
            raise AssertionError('Invalid response')
        if 'error' in res.json():
            raise AssertionError('Invalid login credentials')

    def load(self, code, files):
        self.session.get(url=f'http://{self.host}/engine/jobs/')

        data = {
            'code': code,
            'args': [[file] for file in files],
            'kwargs': [{} for _ in files],
        }

        res = self.session.post(url=f'http://{self.host}/engine/jobs/', headers={
            'X-CSRFToken': self.session.cookies['csrftoken']
        }, json=data)

        if res.status_code != 200:
            raise AssertionError(f'Failed to load job on host {self.host}')

        self.job_id = res.json()['job_id']

    def status(self):
        res = self.session.get(url=f'http://{self.host}/engine/jobs/{self.job_id}/')

        if res.status_code != 200:
            raise AssertionError(f'Failed to load job on host {self.host}')

        data = res.json()
        if data['status'] != 'finished':
            return False

        return data

    def score(self):
        res = self.session.get(url=f'http://{self.host}/engine/score/')

        if res.status_code != 200:
            raise AssertionError(f'Failed to load job on host {self.host}')

        return res.json()['score']

    def join(self):
        while not self.status():
            time.sleep(1)

    def close(self):
        self.session.get(url=f'http://{self.host}/user/logout/')
