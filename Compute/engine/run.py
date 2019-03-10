import os
import pickle
import sys


def run_env(path: str, module: str):
    """
    Runs the environment in the path provided and the python module
    :param path: Path to the job environment
    :param module: Python module name to be run in the environment
    :return:
    :rtype: None
    """
    os.chdir(path)
    with open('out', 'wb') as f:
        f.write(pickle.dumps(None))
    sys.path.insert(1, path)

    with open('inp', 'rb') as f:
        inp = f.read()

    try:
        code = __import__(module)
        out = code.main(inp)

    except Exception as e:
        out = f'{e.__class__.__name__}: {",".join(e.args)}'

    print(out)

    out = pickle.dumps(out)
    with open('out', 'wb') as f:
        f.write(out)


run_env(sys.argv[1], sys.argv[2])
