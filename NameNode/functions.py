import socket


def send(data, address):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    
    try:
        sock.connect(address)
    except (ConnectionRefusedError, OSError) as e:
        print("Type:", type(e), "\nException:", e)
    
    sock.send((str(data) + "-end-").encode("utf-8"))
    
    data = ""
    while True:
        buff = sock.recv(4096)
        if not buff:
            break
        data += buff.decode("utf-8")

    sock.close()
    return eval(data)


def handle(conn, addr, main):
    data = ""
    while True:
        if data[-5:] == "-end-":
            break
        data += conn.recv(4096).decode("utf-8")

    data = eval(data[:-5])
    print(data)
    
    # Do someting with the data
    function_map[data["type"]](data)

    conn.send('{"status": "ack"}'.encode("utf-8"))
    conn.close()


def client_register(data):
    print("register")
    pass


def client_push_code(data):
    print("push code")
    pass


def client_push_files(data):
    print("push files")
    pass


def client_map(data):
    print("map")
    pass


def client_filter(data):
    print("filter")
    pass


def client_reduce(data):
    print("reduce")
    pass


def client_result(data):
    print("result")
    pass


function_map = {
    "00000": client_register,
    "00001": client_push_code,
    "00010": client_push_files,
    "00011": client_map,
    "00100": client_filter,
    "00101": client_reduce,
    "00110": client_result,
}
