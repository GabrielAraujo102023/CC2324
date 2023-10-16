import os
import sys
import socket
import threading

# Check if the correct number of command-line arguments is provided
if len(sys.argv) != 2:
    print("Usage: FS_Node <share_folder>")
    sys.exit(1)

# Extract command-line arguments
share_folder = sys.argv[1]

# Define the address and port for the FS_Tracker server
tracker_host = "tracker_server_ip"
tracker_port = 9090

# Define the UDP port for file transfer
udp_port = 9090


class FS_Node:
    def __init__(self, shared_dir):
        self.shared_dir = shared_dir
        self.node_info = {
            "address": "",
            "files": self.get_files_in_shared_dir(),
        }

    def get_files_in_shared_dir(self):
        files = []
        if os.path.exists(self.shared_dir) and os.path.isdir(self.shared_dir):
            files = [f for f in os.listdir(self.shared_dir) if os.path.isfile(os.path.join(self.shared_dir, f))]
        return files

    # Create a TCP socket to connect to the FS_Tracker server
    def connect_to_tracker(self):
        fs_node_tcp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        try:
            # Connect to the tracker
            fs_node_tcp.connect((tracker_host, tracker_port))

            # Update node_info with the correct address
            self.node_info["address"] = fs_node_tcp.getsockname()

            # Announce presence and share file information with the tracker
            fs_node_tcp.send(str(self.node_info).encode())

            while True:
                # Handle file transfer requests here
                pass

        except Exception as e:
            print(f"Error connecting to the tracker: {e}")

    # Create a UDP socket for file transfer
    def file_transfer(self):
        fs_node_udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        fs_node_udp.bind(('', udp_port))

        while True:
            # Handle incoming file transfer requests via UDP
            data, addr = fs_node_udp.recvfrom(1024)
            # Process the request and transfer the file
            pass


# Run the FS_Node application using threads
if __name__ == "__main__":
    fs_node = FS_Node(share_folder)

    tracker_thread = threading.Thread(target=fs_node.connect_to_tracker)
    tracker_thread.start()

    transfer_thread = threading.Thread(target=fs_node.file_transfer)
    transfer_thread.start()