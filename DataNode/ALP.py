import socket
import threading


class Server():
    def __init__(self, ip="127.0.0.1", port=8000, connections=5):
        
        self.td = []
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_address = (ip, port)
        
        try:
            self.sock.bind(self.server_address)
            self.sock.listen(connections)
        except OSError as e:
            print("Type:", type(e), "\nException:", e)

    def start(self, target):
        
        conn, addr = self.sock.accept()
        # print("Accepted connection from:", addr)
        self.td.append(threading.Thread(target=target, args=(conn, addr,)))
        self.td[-1].start()

    def stop(self):
        
        for t in self.td:
            t.join()
        
        self.sock.close()

class Client():
    def __init__(self):
        pass

    def start(self, target, args, ip, port):
        
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        
        try:
            sock.connect((ip, port))
        except (ConnectionRefusedError, OSError) as e:
            print("Type:", type(e), "\nException:", e)
        
        response = target(sock, *args)
        sock.close()
        return response

def send(data, address):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    
    try:
        sock.connect(address)
    except (ConnectionRefusedError, OSError) as e:
        print("Type:", type(e), "\nException:", e)
    
    sock.send(str(data).encode("utf-8"))
    
    data = ""
    while True:
        buff = sock.recv(4096)
        if not buff:
            break
        data += buff.decode("utf-8")

    sock.close()
    return eval(data)

