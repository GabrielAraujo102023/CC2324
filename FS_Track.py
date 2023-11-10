import socket
import sys
import threading
import json
from dataclasses import dataclass
from typing import Dict


@dataclass
class FileInfo:
    total_blocks: int
    block_owners: dict
    file_hash: str
    available: bool

    def __init__(self, total_blocks, clientIP, file_hash, available=True, block_number=-1):
        self.total_blocks = total_blocks
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
FILE_REQUEST = 1
FILE_UPDATE = 2
BLOCK_UPDATE = 3
BLOCK_REQUEST = 4
FILE_INFO = 5


def main():
    # Criação do Socket
    tcpSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    HOST, _ = tcpSocket.getsockname()
    PORT = 9090
    if len(sys.argv) == 2:  # Verifica se usa um Port costumizadoo
        PORT = int(sys.argv[1])
    tcpSocket.bind((HOST, PORT))
    tcpSocket.listen()
    print("Servidor ativo em " + HOST + " porta " + str(PORT))

    # Fica à espera de conexões novas, cria uma thread para cada nodo que se conecta
    while True:
        print("ESPERANDO")
        clientSocket, (clientIP, clientPORT) = tcpSocket.accept()
        print("Conectado a cliente: " + str(clientIP) + " na porta " + str(clientPORT))
        t = threading.Thread(target=connectionTask, args=[clientSocket, clientIP])
        t.start()


# Função usada pelas threads das nodes
def connectionTask(clientSocket, clientIP):
    message = clientSocket.recv(1024).decode()
    json_message = json.loads(message)
    files_info = json_message["files_info"]
    blocks_info = json_message["blocks_info"]
    # Recebe o nome de todos os ficheiros
    stop = "-1"
    if files_info:
        for file, total_blocks, file_hash in files_info:
            if file in files:
                for owners in files[file].block_owners.values():
                    owners.append(clientIP)
            else:
                new_file = FileInfo(total_blocks, clientIP, file_hash)
                files.update({file: new_file})
    print(files)
    if blocks_info:
        for block_name, file_hash, total_blocks in blocks_info:
            parts = block_name.split("_")
            file = parts[0]
            block_number = int(parts[1])
            if file in files:
                files[file].block_owners[block_number].append(clientIP)
            else:
                new_file = FileInfo(total_blocks, clientIP, file_hash, False, block_number)
                files.update({file: new_file})
    print(files)

    # Verifica se a conexão é fechada ou recebe um nome de um ficheiro e envia todos os nodos associados a este
    # TODO: Adicionar timeouts
    while True:
        message = clientSocket.recv(1024).decode()
        addressList = {}
        print("message -> " + str(message))
        if message == stop:
            try:
                clientSocket.shutdown(socket.SHUT_RDWR)
            except Exception as e:
                print(f"Erro a desativar a socket: {e}")
            clientSocket.close()
            print("Cliente " + str(clientIP) + " desconectou")
            cleanClient(clientIP)
            break
        else:
            jsonMsg = json.loads(message)
            msgType = jsonMsg["type"]
            file_name = jsonMsg["filename"]
            if msgType == FILE_REQUEST:
                if file_name in files:
                    addressList = files[file_name].block_owners
                addressListJson = json.dumps(addressList)
                clientSocket.send(addressListJson.encode())
            elif msgType == BLOCK_UPDATE:
                update_file_info(file_name, clientIP)
                continue
            elif msgType == FILE_INFO:
                file_info = {
                    "type": msgType,
                    "file_hash": files[file_name].file_hash,
                    "total_blocks": files[file_name].total_blocks
                }
                file_info_json = json.dumps(file_info)
                clientSocket.send(file_info_json.encode())


def update_file_info(file, clientIP):
    parts = file.split("_")
    file_name = parts[0]
    block_number = int(parts[1])
    files[file_name].block_owners[block_number].append(clientIP)


def cleanClient(address):
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
