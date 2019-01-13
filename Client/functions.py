import socket


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
