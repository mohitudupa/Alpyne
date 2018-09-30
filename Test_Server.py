import ALP
import json


def recver(conn, addr):
    data = conn.recv(4096).decode("utf-8")
    print(data)
    # json_data = json.dumps(eval(data), indent=4)

    # print(json_data)

    conn.send("ACK".encode("utf-8"))
    conn.close()


server = ALP.Server()
try:
    while(1):
        server.start(recver)
except KeyboardInterrupt:
    server.stop()
