import socket


def send(data, address):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    
    try:
        sock.connect(address)
    except (ConnectionRefusedError, OSError) as e:
        print("Type:", type(e), "\nException:", e)
    print("from datanode.send:", data)
    
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
    print("inside validate")
    try:
        eval(data)
        return True
    except Exception:
        return False


def handle(conn, addr, data_node):
    data = ""
    while True:
        if data[-1:] == "}" and validate(data):
            break
        data += conn.recv(4096).decode("utf-8")

    data = eval(data)
    print(data)
    
    # Do someting with the data
    result = data_node.function_map[data["type"]](data["id"], data["data"])

    conn.send(str(result).encode("utf-8"))
    conn.close()


def getattrs(module_name, module_alias):
    x = __import__(module_name)
    return[(module_alias, i) for i in dir(x) if callable(getattr(x, i))]