import socket
import sys
import threading
import time
from dataclasses import dataclass
from typing import Dict
import message_types
import pickle
from dns import contact_dns


# Classe usada apenas guardar informação de um ficheiro
@dataclass
class FileInfo:
    # Número total de blocos do ficheiro
    total_blocks: int
    # Dicionário que guarda todos os donos dos blocos do ficheiro {número do bloco: [donos]}
    block_owners: Dict[int, list]
    # Hash do ficheiro
    file_hash: str
    # Variável usada para identificar se todos os blocos do ficheiro estão disponíveis na rede
    available: bool
    # Momento em que o ficheiro se torna indisponível na rede
    hide_timestamp: float

    def __init__(self, total_blocks, clientIP, file_hash, available=True, block_number=-1):
        self.total_blocks = int(total_blocks)
        self.block_owners = {}
        block_number = int(block_number)
        # Se for passado um bloco como argumento, adiciona o cliente à lista de donos desse bloco. As listas de donos
        # dos outros blocos ficam vazias. O ficheiro fica indisponível até existir um dono em todos os blocos
        if block_number >= 0:
            for i in range(0, total_blocks):
                if i == block_number:
                    self.block_owners[i] = [clientIP]
                else:
                    self.block_owners[i] = []
        # Usado quando o cliente possui o ficheiro completo. O cliente é adicionado a todas as listas de donos de todos os blocos
        else:
            for i in range(0, total_blocks):
                self.block_owners[i] = [clientIP]
        self.available = available
        self.file_hash = file_hash

        if available:
            self.hide_timestamp = 0
        else:
            self.hide_timestamp = time.time()

    # Marca o ficheiro como indisponível na rede
    def hide_file(self):
        self.available = False

    # Marca o ficheiro como disponível na rede
    def unhide_file(self):
        self.available = True

    # Verifica se todos os blocos do ficheiro estão disponíveis
    def are_all_blocks_available(self):
        return all(owners for owners in self.block_owners.values())


# Dicinário de ficheiros na rede
files: Dict[str, FileInfo] = {}
# Lock usada para aceder a files
files_lock = threading.Lock()
# Intervalo de tempo para executar um cleanup dos ficheiros
CLEANUP_INTERVAL = 15
# Socket UDP usado para comunicações com o DNS
udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
# Dicionário para guardar os nomes dos nodos que se conectam a ele
names = {}


# Limpa ficheiros que estejam marcados como indisponiveís
def cleanup():
    while True:
        # A cada x segundos (x == CLEANUP_INTERVAL)
        time.sleep(CLEANUP_INTERVAL)
        current_timestamp = time.time()

        with files_lock:
            # Filtra ficheiros que estão indisponíveis há um certo tempo
            files_to_delete = [
                file
                for file, file_info in files.items()
                if not file_info.available and (current_timestamp - file_info.hide_timestamp) > CLEANUP_INTERVAL
            ]

            # Elimina os ficheiros
            if files_to_delete:
                for file in files_to_delete:
                    del files[file]
                print("Cleanup executado")
                print(files)


def main():
    tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    HOST, _ = tcp_socket.getsockname()
    PORT = 9090
    udp_socket.bind(('', 9091))
    # Envia ao DNS uma mensagem sem nomes apenas para o DNS guardar o tracker em memória
    contact_dns(socket.gethostname(), udp_socket, [], '')
    if len(sys.argv) == 2:
        PORT = int(sys.argv[1])
    tcp_socket.bind((HOST, PORT))
    # Torna possível ao servidor aceitar conexões
    tcp_socket.listen()
    print("Servidor ativo em " + HOST + " porta " + str(PORT))
    # Cria thread que vai efetuar cleanup periódico
    cleanup_thread = threading.Thread(target=cleanup)
    cleanup_thread.daemon = True
    cleanup_thread.start()

    # Aceita uma nova conexão, cria uma thread e uma socket para cada cliente que se conecta
    while True:
        print("ESPERANDO")
        client_socket, (clientIP, clientPORT) = tcp_socket.accept()
        print("Conectado a cliente: " + str(clientIP) + " na porta " + str(clientPORT))
        t = threading.Thread(target=connection_thread, args=[client_socket, clientIP])
        t.start()


# Função usada pela thread de cada cliente
def connection_thread(client_socket, client_ip):
    client_crashed = True
    try:
        # Fica à espera de receber mensagens do cliente
        while True:
            pickle_message = client_socket.recv(1024)
            # Se não receber sinal nenhum, é porque o cliente crashou
            if not pickle_message:
                break
            try:
                message = pickle.loads(pickle_message)

            except pickle.UnpicklingError as e:
                print(f"Erro a converter mensagem recebida: {e}")

            else:
                print("message -> " + str(message))
                # Mensagem com pedido de donos de blocos de um ficheiro
                if message.type == message_types.MessageType.OWNERS_REQUEST:
                    owners_list = {}
                    file_name = message.file_name

                    if file_name in files and files[file_name].available:
                        for block_number, owners in files[file_name].block_owners.items():
                            for o in owners:
                                # Vai buscar o nome do owner
                                owner = names[o]
                                # Cria dicionário {dono: [blocos que possui]}
                                if owner not in owners_list:
                                    owners_list[owner] = [block_number]
                                else:
                                    owners_list[owner].append(block_number)

                    # Constrói mensagem de resposta e envia
                    response = message_types.OwnersMessage(owners_list)
                    client_socket.send(pickle.dumps(response))

                # Mensagem com novo bloco disponível
                elif message.type == message_types.MessageType.BLOCK_UPDATE:
                    # Atualiza informação do ficheiro
                    update_file_info(message.block_name, client_ip)

                # Mensagem com pedido de informação sobre um ficheiro
                elif message.type == message_types.MessageType.FILE_INFO_REQUEST:
                    file_name = message.file_name
                    # Constrói mensagem de resposta com a hash e o número total de blocos do ficheiro e envia
                    response = message_types.FileInfoMessage(files[file_name].file_hash, files[file_name].total_blocks)
                    client_socket.send(pickle.dumps(response))

                # Mensagem com informações da pasta partilhada pelo cliente
                elif message.type == message_types.MessageType.NEW_CONNECTION:
                    names[client_ip] = message.node_name
                    # Processa e guarda a informação recebida
                    new_connection_info(message.files_info, message.blocks_info, client_ip)

                # Mensagem de desconexão
                elif message.type == message_types.MessageType.DISCONNECT:
                    try:
                        # Encerra a socket do cliente
                        client_socket.shutdown(socket.SHUT_RDWR)
                    except socket.error as e:
                        print(f"Erro a desativar a socket: {e}")
                    client_socket.close()
                    # Elimina os dados do cliente
                    clean_client(client_ip)
                    print(f"Cliente {client_ip} desconetou-se. Dados eliminados")
                    client_crashed = False
                    print(files)
                    break
    finally:
        # Deteta se um cliente crashou. Encerra a conexão e elimina os dados do cliente
        if client_crashed:
            try:
                client_socket.shutdown(socket.SHUT_RDWR)
            except socket.error as e:
                print(f"Error shutting down socket: {e}")
            client_socket.close()
            clean_client(client_ip)
            print(f"Cliente {client_ip} crashou. Dados eliminados")
            print(files)


# Processa uma mensagem com informação da pasta partilhada pelo cliente
def new_connection_info(files_info, blocks_info, client_ip):
    with files_lock:
        if files_info:
            for file, total_blocks, file_hash in files_info:
                # Se o ficheiro já existir na rede, acrescenta cliente à lista de donos
                if file in files:
                    for owners in files[file].block_owners.values():
                        owners.append(client_ip)
                        if not files[file].available:
                            files[file].unhide_file()
                # Se não, adiciona o novo ficheiro
                else:
                    new_file = FileInfo(total_blocks, client_ip, file_hash)
                    files.update({file: new_file})

        if blocks_info:
            for block_name, file_hash, total_blocks in blocks_info:
                parts = block_name.split("_")
                file = parts[0]
                block_number = int(parts[1])

                # Se o ficheiro existir na rede mas estiver indisponível, adiciona o cliente aos donos dos blocos que este possui
                if file in files:
                    files[file].block_owners[block_number].append(client_ip)
                    # Se os blocos ficarem todos disponíveis, marca o ficheiro como disponível
                    if files[file].are_all_blocks_available():
                        files[file].unhide_file()
                # Se o ficheiro não existir na rede, cria o ficheiro e adiciona o cliente aos donos dos blocos que este
                # possui, marcando o ficheiro como indisponível
                else:
                    new_file = FileInfo(total_blocks, client_ip, file_hash, False, block_number)
                    files.update({file: new_file})
        print(files)


# Adicina cliente à lista de clientes que possui o bloco
def update_file_info(block_name, clientIP):
    with files_lock:
        parts = block_name.split("_")
        file_name = parts[0]
        block_number = int(parts[1])
        files[file_name].block_owners[block_number].append(clientIP)


# Elimina dados de um cliente
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

        # Apaga o respetivo nome da memória
        if address in names:
            names.pop(address)


if __name__ == '__main__':
    main()
