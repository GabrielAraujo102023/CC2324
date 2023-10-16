import os
import sys
import socket
import threading
import hashlib

if len(sys.argv) != 3:
    print("Usage: FS_Node <share_folder> <address> <port>")
    sys.exit(1)

shared_folder = sys.argv[1]
tracker_host = sys.argv[2]
tracker_port = 9090
if len(sys.argv) == 4:
    tracker_port = sys.argv[3]
udp_port = 9090
block_size = 128

class FS_Node:
    def __init__(self):
        self.available_blocks = {}  # Dictionary to track block availability
        self.is_file_divided()
        self.node_info = {
            "address": "",
            "blocks": self.available_blocks,
        }

    def connect_to_tracker(self):
        fs_node_tcp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        try:
            fs_node_tcp.connect((tracker_host, tracker_port))

            # Update node_info with the correct address
            self.node_info["address"] = fs_node_tcp.getsockname()

            fs_node_tcp.send(str(self.node_info).encode())

            while True:
                # Handle file transfer requests here
                pass

        except Exception as e:
            print(f"Error connecting to the tracker: {e}")

    def is_file_divided(self):
        for root, _, files in os.walk(shared_folder):
            for file in files:
                file_path = os.path.join(root, file)
                if not os.path.isfile(file_path):
                    continue
                self.divide_file_into_blocks(file_path)

    def divide_file_into_blocks(self, file_path):
        with open(file_path, 'rb') as file:
            block_number = 0
            while True:
                data = file.read(block_size)
                if not data:
                    break
                block_id = hashlib.md5(data).hexdigest()  # Unique identifier for the block
                self.available_blocks[block_id] = {
                    "owner": self.node_info["address"],
                    "available": True,
                }
                block_path = os.path.join(shared_folder, block_id)
                with open(block_path, 'wb') as block_file:
                    block_file.write(data)
                block_number += 1

    def file_transfer(self):
        fs_node_udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        fs_node_udp.bind(('', udp_port))

        while True:
            # Handle incoming file transfer requests via UDP
            data, addr = fs_node_udp.recvfrom(1024)
            # Process the request and transfer the file
            pass

if __name__ == "__main__":
    fs_node = FS_Node()

    tracker_thread = threading.Thread(target=fs_node.connect_to_tracker)
    tracker_thread.start()

    transfer_thread = threading.Thread(target=fs_node.file_transfer)
    transfer_thread.start()