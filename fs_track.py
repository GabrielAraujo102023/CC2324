import socket
import sys
import threading
import time
from dataclasses import dataclass
from typing import Dict
import message_types
import pickle


@dataclass
class FileInfo:
    total_blocks: int
    block_owners: Dict[int, list]
    file_hash: str
    available: bool
    hide_timestamp: float

    def __init__(self, total_blocks, clientIP, file_hash, available=True, block_number=-1):
        self.total_blocks = int(total_blocks)
        self.block_owners = {}
        block_number = int(block_number)
        if block_number >= 0:
            for i in range(0, total_blocks):
                if i == block_number:
                    self.block_owners[i] = [clientIP]
                else:
                    self.block_owners[i] = []
        else:
            for i in range(0, total_blocks):
                self.block_owners[i] = [clientIP]
        self.available = available
        self.file_hash = file_hash
        if available:
            self.hide_timestamp = 0
        else:
            self.hide_timestamp = time.time()

    def hide_file(self):
        self.available = False

    def unhide_file(self):
        self.available = True

    def are_all_blocks_available(self):
        return all(owners for owners in self.block_owners.values())


files: Dict[str, FileInfo] = {}
files_lock = threading.Lock()
CLEANUP_INTERVAL = 15


def cleanup():
    while True:
        time.sleep(CLEANUP_INTERVAL)
        current_timestamp = time.time()

        with files_lock:
            files_to_delete = [
                file
                for file, file_info in files.items()
                if not file_info.available and (current_timestamp - file_info.hide_timestamp) > CLEANUP_INTERVAL
            ]

            if files_to_delete:
                for file in files_to_delete:
                    del files[file]
                print("APAGUEI FICHEIROS ESCONDIDOS HÁ TEMPO A MAIS MY BROTHER")
                print(files)


def main():
    # Criação do Socket
    tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    HOST, _ = tcp_socket.getsockname()
    PORT = 9090
    if len(sys.argv) == 2:  # Verifica se usa um Port costumizadoo
        PORT = int(sys.argv[1])
    tcp_socket.bind((HOST, PORT))
    tcp_socket.listen()
    print("Servidor ativo em " + HOST + " porta " + str(PORT))
    cleanup_thread = threading.Thread(target=cleanup)
    cleanup_thread.daemon = True
    cleanup_thread.start()

    # Fica à espera de conexões novas, cria uma thread para cada nodo que se conecta
    while True:
        print("ESPERANDO")
        client_socket, (clientIP, clientPORT) = tcp_socket.accept()
        print("Conectado a cliente: " + str(clientIP) + " na porta " + str(clientPORT))
        t = threading.Thread(target=connection_thread, args=[client_socket, clientIP])
        t.start()


# Função usada pelas threads das nodes
def connection_thread(client_socket, client_ip):
    client_crashed = True
    try:
        while True:
            pickle_message = client_socket.recv(1024)
            if not pickle_message:
                break
            try:
                message = pickle.loads(pickle_message)
            except pickle.UnpicklingError as e:
                print(f"Erro a converter mensagem recebida: {e}")
            else:
                print("message -> " + str(message))
                if message.type == message_types.MessageType.OWNERS_REQUEST:
                    owners = {}
                    file_name = message.file_name
                    if file_name in files and files[file_name].available:
                        owners = files[file_name].block_owners.copy()
                        print(owners)
                    response = message_types.OwnersMessage(owners)
                    client_socket.send(pickle.dumps(response))
                elif message.type == message_types.MessageType.BLOCK_UPDATE:
                    update_file_info(message.block_name, client_ip)
                elif message.type == message_types.MessageType.FILE_INFO_REQUEST:
                    file_name = message.file_name
                    response = message_types.FileInfoMessage(files[file_name].file_hash, files[file_name].total_blocks)
                    client_socket.send(pickle.dumps(response))
                elif message.type == message_types.MessageType.NEW_CONNECTION:
                    new_connection_info(message.files_info, message.blocks_info, client_ip)
                elif message.type == message_types.MessageType.DISCONNECT:
                    try:
                        client_socket.shutdown(socket.SHUT_RDWR)
                    except socket.error as e:
                        print(f"Erro a desativar a socket: {e}")
                    client_socket.close()
                    print("Cliente " + str(client_ip) + " desconectou")
                    clean_client(client_ip)
                    client_crashed = False
                    print(files)
                    break
    finally:
        if client_crashed:
            print("CLIENTE FOI COM O CARALHO")
            try:
                client_socket.shutdown(socket.SHUT_RDWR)
            except socket.error as e:
                print(f"Error shutting down socket: {e}")
            client_socket.close()
            clean_client(client_ip)
            print(f"Cliente {client_ip} DESCONECTADO E LIMPO")
            print(files)


def new_connection_info(files_info, blocks_info, client_ip):
    with files_lock:
        if files_info:
            for file, total_blocks, file_hash in files_info:
                if file in files:
                    for owners in files[file].block_owners.values():
                        owners.append(client_ip)
                        files[file].unhide_file()
                else:
                    new_file = FileInfo(total_blocks, client_ip, file_hash)
                    files.update({file: new_file})
        # print(files)
        if blocks_info:
            for block_name, file_hash, total_blocks in blocks_info:
                parts = block_name.split("_")
                file = parts[0]
                block_number = int(parts[1])
                if file in files:
                    files[file].block_owners[block_number].append(client_ip)
                    if files[file].are_all_blocks_available():
                        files[file].unhide_file()
                else:
                    new_file = FileInfo(total_blocks, client_ip, file_hash, False, block_number)
                    files.update({file: new_file})
        print(files)


def update_file_info(block_name, clientIP):
    with files_lock:
        parts = block_name.split("_")
        file_name = parts[0]
        block_number = int(parts[1])
        files[file_name].block_owners[block_number].append(clientIP)


def clean_client(address):
    with files_lock:
        del_list = []
        hide_list = []

        for file, file_info in files.items():
            for owners in file_info.block_owners.values():
                if address in owners:
                    owners.remove(address)
            if not file_info.are_all_blocks_available():
                hide_list.append(file)

        for file in hide_list:
            files[file].hide_file()
        for file in del_list:
            del files[file]


if __name__ == '__main__':
    main()
