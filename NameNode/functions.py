import socket
import os


def send(data, address):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    
    try:
        sock.connect(address)
    except (ConnectionRefusedError, OSError) as e:
        print("Type:", type(e), "\nException:", e)
    
    sock.send((str(data)).encode("utf-8"))
    
    data = ""
    while True:
        buff = sock.recv(4096)
        if not buff:
            break
        data += buff.decode("utf-8")

    sock.close()
    return eval(data)


def validate(data):
    try:
        eval(data)
        return True
    except Exception:
        return False


def handle(conn, addr, name_node):
    data = ""
    while True:
        if data[-1:] == "}" and validate(data):
            break
        data += conn.recv(4096).decode("utf-8")

    data = eval(data)
    
    # Do someting with the data
    result = name_node.function_map[data["type"]](data["id"], data["data"])

    conn.send(str(result).encode("utf-8"))
    conn.close()

