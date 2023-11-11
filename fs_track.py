import socket
import sys
import threading
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

    def change_availability(self):
        self.available = not self.available

    def are_all_blocks_available(self):
        return all(bool(owners) for owners in self.block_owners.values())


files: Dict[str, FileInfo] = {}
files_lock = threading.Lock()


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

    # Fica à espera de conexões novas, cria uma thread para cada nodo que se conecta
    while True:
        print("ESPERANDO")
        client_socket, (clientIP, clientPORT) = tcp_socket.accept()
        print("Conectado a cliente: " + str(clientIP) + " na porta " + str(clientPORT))
        t = threading.Thread(target=connection_task, args=[client_socket, clientIP])
        t.start()


# Função usada pelas threads das nodes
def connection_task(client_socket, client_ip):
    # Verifica se a conexão é fechada ou recebe um nome de um ficheiro e envia todos os nodos associados a este
    while True:
        pickle_message = client_socket.recv(1024)
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
                continue
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
                break


def new_connection_info(files_info, blocks_info, client_ip):
    with files_lock:
        if files_info:
            for file, total_blocks, file_hash in files_info:
                if file in files:
                    for owners in files[file].block_owners.values():
                        owners.append(client_ip)
                else:
                    new_file = FileInfo(total_blocks, client_ip, file_hash)
                    files.update({file: new_file})
        print(files)
        if blocks_info:
            for block_name, file_hash, total_blocks in blocks_info:
                parts = block_name.split("_")
                file = parts[0]
                block_number = int(parts[1])
                if file in files:
                    files[file].block_owners[block_number].append(client_ip)
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
            files[file].change_availability()
        for file in del_list:
            del files[file]


if __name__ == '__main__':
    main()
