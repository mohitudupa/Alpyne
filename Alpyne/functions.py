from functools import wraps, reduce
import pickle


def upload_dataset(data, inp, containers):
    pickled_data = [pickle.dumps(x) for x in data]

    containers.open(inp)
    containers.put(pickled_data)


def download_dataset(out, containers):
    containers.open(out)
    pickled_data = containers.get(containers.list_files())

    data = [pickle.load(x) for x in pickled_data]

    return data


def grid_map(func):
    @wraps(func)
    def wrapper(inp: str, out: str, file: str, containers):
        containers.open(inp)
        meta = containers.get_one(file)
        arg = pickle.load(meta)

        res = func(arg)

        containers.open(out)
        meta = pickle.dumps(res)
        containers.put_one(meta, file)

    return wrapper


def grid_filter(func):
    @wraps(func)
    def wrapper(inp, out, file, containers):
        containers.open(inp)
        meta = containers.get_one(file).read()
        arg = pickle.loads(meta)

        res = func(arg)

        if res:
            containers.open(out)
            containers.put_one(meta, file)

    return wrapper


def grid_reduce(func):
    @wraps(func)
    def wrapper(inp, out, files, containers):
        containers.open(inp)
        meta = containers.get_one(files)
        args = pickle.load(meta)

        res = reduce(func, args)

        containers.open(out)
        meta = pickle.dumps(res)
        containers.put_one(meta)

    return wrapper
