import base64
import os
import subprocess
import sys
import socket
import threading
import json
import re
# from dataclasses import dataclass
from typing import Dict, List
import select
import hashlib

if len(sys.argv) < 3 or len(sys.argv) > 4:
    print("Usage: FS_Node <share_folder> <server_address> <port>")
    sys.exit(1)

"""
@dataclass
class BlockInfo:
    total_blocks: int
    block_number: int
    occupied: bool

    def __init__(self, total_blocks, block_number):
        self.total_blocks = total_blocks
        self.block_number = block_number
        self.occupied = False

    def change_occupy_state(self):
        self.occupied = not self.occupied
"""

os.environ['TERM'] = 'xterm'
shared_folder = sys.argv[1]
tracker_host = sys.argv[2]
tracker_port = 9090
if len(sys.argv) == 4:
    tracker_port = sys.argv[3]
udp_port = 9090
socket_tcp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
socket_udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
exit_flag = False
block_size = 512
_, _, files = os.walk(shared_folder)
# blocks_available: Dict[str, BlockInfo] = {}
blocks_available: Dict[str, Dict[int, bool]] = {}
FILE_REQUEST_SERVER = 1
BLOCK_UPDATE = 2
BLOCK_REQUEST_NODE = 3
BLOCK = 4
FILE_INFO = 5
BLOCK_REQUEST_SERVER = 6
local_address = ""
temp = os.path.join(shared_folder, "temp")


def main():
    udp_thread = threading.Thread(target=wait_for_file_request)
    udp_thread.start()
    connect_to_tracker()

    while not exit_flag:
        # clear_terminal()
        print("1 - Transferir ficheiro\n"
              "2 - Fechar conexão\n"
              "Escolha:")
        choice = input()
        if choice == "1":
            askForFile()
        elif choice == "2":
            disconnect()
    udp_thread.join()
    print(f"DESCONETADO COM SUCESSO!")


def wait_for_file_request():
    global local_address
    try:
        local_address, _ = socket_udp.getsockname()
        socket_udp.bind((local_address, udp_port))
        print(f"FS Transfer Protocol: à escuta em {local_address} na porta UDP {udp_port} ")

        while not exit_flag:
            ready, _, _ = select.select([socket_udp], [], [], 3)
            for _ in ready:
                data, addr = socket_udp.recvfrom(1024)
                message = json.loads(data.decode())
                # print("MESSAGE -> " + str(message))
                if message["type"] == BLOCK_REQUEST_NODE:
                    # print(f"RECEBI UMA MENSAGEM COM UM PEDIDO DE FICHEIRO")
                    t = threading.Thread(target=send_file, args=[message["filename"], message["blocks"], addr])
                    t.start()
                elif message["type"] == BLOCK:
                    # print(f"RECEBI UMA MENSAGEM COM UM FICHEIRO")
                    t = threading.Thread(target=receive_file, args=[message["block_name"], message["block_data"]])
                    t.start()
    except Exception as e:
        print(f"ERRO NA PORTA UDP: {e}")
    socket_udp.close()
    print(f"PORTA UDP FECHADA")


def receive_file(block_name, block_data):
    data = block_data.encode()
    save_path = os.path.join(temp, block_name)

    with open(save_path, 'wb') as file:
        file.write(data)

    update_tracker(block_name)

    file_name, block_number = block_name.split("_")

    if file_name in blocks_available:
        blocks_available[file_name].update({block_number: False})
    else:
        blocks_available.update({file_name: {block_number: False}})

    if check_if_all_blocks_available(file_name):
        mount_file(file_name)


def update_tracker(block_name):
    message = {
        "type": BLOCK_UPDATE,
        "block_name": block_name
    }
    try:
        socket_tcp.sendto(json.dumps(message).encode(), tracker_host)
    except Exception as e:
        print(f"ERRO AO ATUALIZAR FICHEIRO NO TRACKER: {e}")


def send_file(filename, blocks, addr):
    if filename in files:
        for block in blocks:
            block_name = filename + "_" + str(block)
            file_path = os.path.join(temp, block_name)

            with open(file_path, 'rb') as file:
                file.seek(block * block_size)
                block_data = file.read(block_size)

            message = {
                "type": BLOCK,
                "block_name": block_name,
                "block_data": block_data.decode()
            }

            try:
                socket_udp.sendto(json.dumps(message).encode(), addr)
                # print(f"Ficheiro enviado")
            except Exception as e:
                print(f"ERRO AO ENVIAR BLOCO: {e}")

    elif filename in blocks_available:

        for block in blocks:
            block_name = filename + "_" + str(block)
            file_path = os.path.join(temp, block_name)

            lock_blocks(blocks, True)

            with open(file_path, 'rb') as file:
                block_data = file.read()

            lock_blocks(blocks, False)

            message = {
                "type": BLOCK,
                "block_name": block_name,
                "block_data": block_data.decode()
            }

            try:
                socket_udp.sendto(json.dumps(message).encode(), addr)
            except Exception as e:
                print(f"ERRO AO ENVIAR BLOCO: {e}")


def connect_to_tracker():
    try:
        socket_tcp.connect((tracker_host, int(tracker_port)))
        files_info = []
        blocks_info = []

        for file in files:
            file_hash = calculate_file_hash(os.path.join(shared_folder, file))
            total_blocks = calculate_block_number(file)
            files_info.append((file, total_blocks, file_hash))

        if os.path.exists(temp):
            for _, _, blocks in os.walk(temp):
                for block in blocks:
                    parts = block.split("_")
                    if parts[1] == "info":
                        continue
                    file_name = parts[0]
                    block_number = int(parts[1])
                    file_info_path = os.path.join(temp, file_name + "_info")
                    with open(file_info_path, 'r') as file_info:
                        file_hash = file_info.readline()
                        total_blocks = file_info.readline()

                    blocks_info.append((block, file_hash, total_blocks))
                    blocks_available.update({file_name: {block_number: False}})
        else:
            os.makedirs(temp)

        message = {
            "files_info": files_info,
            "blocks_info": blocks_info
        }
        socket_tcp.send(json.dumps(message).encode())
        print("Conexão FS Track Protocol com servidor " + tracker_host + " porta " + str(tracker_port))

    except Exception as e:
        print(f"Error connecting to the tracker: {e}")


def calculate_block_number(file):
    try:
        file_path = os.path.join(shared_folder, file)
        file_size = os.path.getsize(file_path)
        total_blocks = file_size / block_size
        if file_size % block_size != 0:
            total_blocks += 1
        return total_blocks
    except Exception as e:
        print(f"ERRO A CALCULAR NUMERO DE BLOCOS DO FICHEIRO: {e}")


def askForFile():
    filename = input("Que ficheiro quer?: ")
    if filename not in files:
        request = {
            "type": FILE_REQUEST_SERVER,
            "filename": filename
        }
        socket_tcp.send(json.dumps(request).encode())
        message = socket_tcp.recv(1024)
        owners_by_block = json.loads(message.decode())
        if owners_by_block:
            print("Bloco do ficheiro -> Lista de clientes que o possui:")
            # {IP: [BLOCOS]}
            blocks_by_client: Dict[str, List[int]] = {}
            for block, clients in owners_by_block.items():
                if filename in blocks_available and block in blocks_available[filename]:
                    continue
                print(f"{block} -> {clients}")
                best_client = choose_best_client(clients)
                if best_client not in blocks_by_client.keys():
                    blocks_by_client.update({best_client: [block]})
                else:
                    blocks_by_client[best_client].append(block)
            input("Pressionar Enter para comecar transferencia...")
            request_file_info = {
                "type": FILE_INFO,
                "filename": filename
            }

            socket_tcp.send(json.dumps(request_file_info).encode())
            response = socket_tcp.recv(1024)
            file_info = json.loads(response.decode())
            file_info_path = os.path.join(temp, (filename + "_info"))
            with open(file_info_path, 'w') as file:
                file.write(file_info["file_hash"]+"\n"+str(file_info["total_blocks"]))

            for client, blocks in blocks_by_client.items():
                message = {
                    "type": BLOCK_REQUEST_NODE,
                    "filename": filename,
                    "blocks": blocks
                }
                socket_udp.sendto(json.dumps(message).encode(), (client, 9090))

        else:
            print("Ficheiro nao encontrado")
            input("Pressionar Enter para voltar ao menu...")
    else:
        print("Ja possui o ficheiro pedido")
        input("Pressionar Enter para voltar ao menu...")


def get_latency(ip):
    # Utiliza pings para verificar velocidade de conexão
    try:
        ping_output = subprocess.check_output(['ping', '-c', '4', ip])
        ping_output = ping_output.decode('utf-8')

        # Calcula a média do RTT
        match = re.search(r'(\d+\.\d+)/\d+\.\d+/\d+\.\d+/\d+\.\d+', ping_output)
        if match:
            latency = float(match.group(1))
            return latency
    except subprocess.CalledProcessError:
        pass

    # Em caso de erro
    return -1


def choose_best_client(ip_list):
    # Ordena IPs por velocidade de conexão
    sorted_ips = sorted(ip_list, key=get_latency)
    print("sorted ips = " + str(sorted_ips))
    return sorted_ips[0]


def disconnect():
    global exit_flag
    try:
        socket_tcp.send("-1".encode())
        socket_tcp.shutdown(socket.SHUT_RDWR)
        socket_tcp.close()
        exit_flag = True
    except Exception as e:
        print(f"Erro ao desconectar: {e}")


def clear_terminal():
    subprocess.call("clear", shell=True)


def calculate_file_hash(file_path):
    # Create a hash object based on the specified algorithm
    hash_object = hashlib.new("sha256")
    # Open the file in binary mode and read it in chunks to efficiently handle large files
    with open(file_path, 'rb') as file:
        while True:
            data = file.read(4096)  # Read data in 64KB chunks (adjust the chunk size as needed)
            if not data:
                break
            hash_object.update(data)

    # Return the hexadecimal representation of the hash
    return hash_object.hexdigest()


if __name__ == "__main__":
    main()
