import os
import sys
import socket
import threading
import hashlib

if len(sys.argv) < 3 or len(sys.argv) > 4:
    print("Usage: FS_Node <share_folder> <address> <port>")
    sys.exit(1)

shared_folder = sys.argv[1]
tracker_host = sys.argv[2]
tracker_port = 9090
if len(sys.argv) == 4:
    tracker_port = sys.argv[3]
udp_port = 9090
block_size = 128
available_blocks = {}
node_info = {
    "address": "",
    "blocks": available_blocks,
}


def connect_to_tracker():
    fs_node_tcp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        print(tracker_host)
        print(tracker_port)
        print("conectado")
        fs_node_tcp.connect((tracker_host, int(tracker_port)))

        # Update node_info with the correct address
        node_info["address"] = fs_node_tcp.getsockname()

        fs_node_tcp.send(fs_node_tcp.getsockname()[0].encode())
        print("mandou")
        for file in node_info["blocks"]:
            fs_node_tcp.send(file)
        fs_node_tcp.send("-1".encode())

        while True:
            os.system("clear")
            print("1 - Pedir ficheiro\n"
                  "2 - Fechar conexão")
            choice = input()
            if choice == "1":
                askForFile(fs_node_tcp)
            elif choice == "2":
                disconnect(fs_node_tcp)
    except Exception as e:
        print(f"Error connecting to the tracker: {e}")


def is_file_divided():
    for root, _, files in os.walk(shared_folder):
        for file in files:
            file_path = os.path.join(root, file)
            if not os.path.isfile(file_path):
                continue
            divide_file_into_blocks(file_path)


def divide_file_into_blocks(file_path):
    with open(file_path, 'rb') as file:
        block_number = 0
        while True:
            data = file.read(block_size)
            if not data:
                break
            block_id = hashlib.md5(data).hexdigest()  # Unique identifier for the block
            available_blocks[block_id] = {
                "owner": node_info["address"],
                "available": True,
            }
            block_path = os.path.join(shared_folder, block_id)
            with open(block_path, 'wb') as block_file:
                block_file.write(data)
            block_number += 1


def file_transfer():
    fs_node_udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    fs_node_udp.bind(("10.0.0.11", udp_port))

    while True:
        # Handle incoming file transfer requests via UDP
        data, addr = fs_node_udp.recvfrom(1024)
        # Process the request and transfer the file
        pass


def askForFile(tcpSocket):
    os.system("clear")
    filename = input("Que ficheiro quer?: ")
    tcpSocket.send(filename)
    message = tcpSocket.recv(1024)
    nodes = []
    while message != "-1":
        nodes.append(message)
        message = tcpSocket.recv(1024)


def disconnect(tcpSocket):
    tcpSocket.send("-1")
    tcpSocket.shutdown()
    tcpSocket.close()


if __name__ == "__main__":
    is_file_divided()

    tracker_thread = threading.Thread(target=connect_to_tracker)
    tracker_thread.start()

# transfer_thread = threading.Thread(target=file_transfer)
# transfer_thread.start()
