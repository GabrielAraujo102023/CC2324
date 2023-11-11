import hashlib
import os
import subprocess
import sys
import socket
import threading
import re
import time
from typing import Dict, List
import select
import message_types
import pickle

files_lock = threading.Lock()


def read_sys_files(folder):
    with files_lock:
        sys_files = []
        for _, _, filenames in os.walk(folder):
            for file in filenames:
                sys_files.append(file)
        return sys_files


os.environ['TERM'] = 'xterm'
SHARED_FOLDER = sys.argv[1]
tracker_ip = sys.argv[2]
tracker_port = 9090
if len(sys.argv) == 4:
    tracker_port = sys.argv[3]
udp_port = 9090
tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
EXIT_FLAG = False
BLOCK_SIZE = 512
files = read_sys_files(SHARED_FOLDER)
# blocks_available: {"filename": {blocknumber: bool}}
blocks_available: Dict[str, Dict[int, bool]] = {}
blocks_available_lock = threading.Lock()
LOCAL_ADRRESS = ""
TEMP_PATH = os.path.join(SHARED_FOLDER, "temp")
BUFFER_SIZE = 1024


def main():
    data_transfer_thread = threading.Thread(target=data_transfer)
    data_transfer_thread.start()
    connect_to_tracker()

    while not EXIT_FLAG:
        # clear_terminal()
        print("1 - Transferir ficheiro\n"
              "2 - Fechar conexão\n"
              "Escolha:")
        choice = input()
        if choice == "1":
            ask_for_file()
        elif choice == "2":
            disconnect()

    data_transfer_thread.join()
    print(f"DESCONETADO COM SUCESSO!")


def data_transfer():
    global LOCAL_ADRRESS
    try:
        LOCAL_ADRRESS, _ = udp_socket.getsockname()
        udp_socket.bind((LOCAL_ADRRESS, udp_port))
        print(f"FS Transfer Protocol: à escuta em {LOCAL_ADRRESS} na porta UDP {udp_port} ")

        while not EXIT_FLAG:
            ready, _, _ = select.select([udp_socket], [], [], 3)
            for _ in ready:
                pickle_message, requester_ip = udp_socket.recvfrom(BUFFER_SIZE)
                try:
                    message = pickle.loads(pickle_message)
                except pickle.UnpicklingError as e:
                    print(f"Erro a converter mensagem recebida: {e}")
                else:
                    # print("MESSAGE -> " + str(message))
                    if message.type == message_types.MessageType.BLOCK_REQUEST:
                        # print(f"RECEBI UMA MENSAGEM COM UM PEDIDO DE FICHEIRO")
                        t = threading.Thread(target=send_block, args=[message.file_name, message.blocks, requester_ip])
                        t.start()
                    elif message.type == message_types.MessageType.BLOCK:
                        # print(f"RECEBI UMA MENSAGEM COM UM FICHEIRO")
                        t = threading.Thread(target=receive_block, args=[message.block_name, message.block_data, message.block_hash])
                        t.start()
    except Exception as e:
        print(f"ERRO NA PORTA UDP: {e}")
    udp_socket.close()
    print(f"PORTA UDP FECHADA")


def receive_block(block_name, block_data, block_hash):
    save_path = os.path.join(TEMP_PATH, block_name)
    global files

    if calculate_block_hash(block_data) == block_hash:
        print("RECEBI BEM OS BYTES DO BLOCO. AS HASHS SAO IGUAIS")
        with open(save_path, 'wb') as file:
            file.write(block_data)
    else:
        print("O BLOCO RECEBIDO É DIFERENTE DO ENVIADO MY BOY")

    file_name, block_number = block_name.split("_")

    with blocks_available_lock:
        if file_name in blocks_available:
            blocks_available[file_name].update({block_number: False})
        else:
            blocks_available.update({file_name: {block_number: False}})

    all_blocks_available, file_hash = check_blocks_available(file_name)

    if all_blocks_available:
        temp_blocks = mount_file(file_name, file_hash)
        files = read_sys_files(SHARED_FOLDER)
        update_tracker(block_name)
        delete_temp_blocks(temp_blocks)
    else:
        update_tracker(block_name)


def delete_temp_blocks(temp_blocks):
    aux = temp_blocks.copy()
    file_name = ""

    while aux:
        for block in temp_blocks:
            file_name, block_number = block.split("_")
            if blocks_available[file_name][block_number]:
                continue
            else:
                block_path = os.path.join(TEMP_PATH, block)
                os.remove(block_path)
                aux.remove(block)

    os.remove(os.path.join(TEMP_PATH, file_name + "_info"))


def mount_file(file_name, file_hash):
    file_blocks = []
    blocks = read_sys_files(TEMP_PATH)

    for block in blocks:
        block_name, block_number = block.split("_")
        if block_name != file_name or block_number == "info":
            continue
        else:
            file_blocks.append(block)

    file_blocks = sorted(file_blocks)
    print(f"LISTA DE BLOCOS: {file_blocks}")
    mounted_file_path = os.path.join(SHARED_FOLDER, file_name)

    mount_complete = False
    while not mount_complete:
        try:
            with open(mounted_file_path, 'wb') as mounted_file:

                for block in file_blocks:
                    block_path = os.path.join(TEMP_PATH, block)

                    with open(block_path, 'rb') as block_data:
                        block_info = block_data.read()
                        print("LI O QUE ESTAVA NO BLOCO")
                        mounted_file.write(block_info)

            if calculate_file_hash(mounted_file_path) != file_hash:
                print("MAL MONTADO VOU TENTAR OUTRA VEZ")
                raise Exception
        except Exception as e:
            print(e)
            os.remove(mounted_file_path)
            time.sleep(1)
        else:
            print("FICHEIRO BEM MONTADO")
            mount_complete = True

    return file_blocks


def check_blocks_available(file_name):
    file_path = os.path.join(TEMP_PATH, file_name + "_info")
    with open(file_path, 'r') as file_info:
        file_hash = file_info.readline().strip()
        total_blocks = int(file_info.readline())
    with blocks_available_lock:
        return len(blocks_available[file_name]) == total_blocks, file_hash


def update_tracker(block_name):
    block_update = message_types.BlockUpdateMessage(block_name)

    try:
        tcp_socket.send(pickle.dumps(block_update))
    except Exception as e:
        print(f"ERRO AO ATUALIZAR FICHEIRO NO TRACKER: {e}")


def send_block(filename, blocks, requester_ip):
    with files_lock and blocks_available_lock:
        if filename in files:
            for block in blocks:
                block_name = filename + "_" + str(block)
                file_path = os.path.join(SHARED_FOLDER, filename)

                with open(file_path, 'rb') as file:
                    file.seek(block * BLOCK_SIZE)
                    block_data = file.read(BLOCK_SIZE)

                block_hash = calculate_block_hash(block_data)
                block_message = message_types.BlockMessage(block_name, block_data, block_hash)

                try:
                    udp_socket.sendto(pickle.dumps(block_message), requester_ip)
                    # print(f"Ficheiro enviado")
                except Exception as e:
                    print(f"ERRO AO ENVIAR BLOCO: {e}")
        elif filename in blocks_available:
            for block in blocks:
                blocks_available[filename][block] = True

            for block in blocks:
                block_name = filename + "_" + str(block)
                file_path = os.path.join(TEMP_PATH, block_name)

                with open(file_path, 'rb') as file:
                    block_data = file.read()

                blocks_available[filename][block] = False

                block_hash = calculate_block_hash(block_data)
                block_message = message_types.BlockMessage(block_name, block_data, block_hash)

                try:
                    udp_socket.sendto(pickle.dumps(block_message), requester_ip)
                except Exception as e:
                    print(f"ERRO AO ENVIAR BLOCO: {e}")


def connect_to_tracker():
    try:
        tcp_socket.connect((tracker_ip, int(tracker_port)))
        files_info = []
        blocks_info = []

        for file in files:
            file_hash = calculate_file_hash(os.path.join(SHARED_FOLDER, file))
            total_blocks = calculate_blocks_number(file)
            files_info.append((file, total_blocks, file_hash))

        if os.path.exists(TEMP_PATH):
            for _, _, blocks in os.walk(TEMP_PATH):
                for block in blocks:
                    parts = block.split("_")
                    if parts[1] == "info":
                        continue
                    file_name = parts[0]
                    block_number = int(parts[1])
                    file_info_path = os.path.join(TEMP_PATH, file_name + "_info")
                    with open(file_info_path, 'r') as file_info:
                        file_hash = file_info.readline()
                        total_blocks = file_info.readline()

                    blocks_info.append((block, file_hash, total_blocks))
                    with blocks_available_lock:
                        blocks_available.update({file_name: {block_number: False}})
        else:
            os.makedirs(TEMP_PATH)

        new_connection = message_types.NewConnectionMessage(files_info, blocks_info)
        tcp_socket.send(pickle.dumps(new_connection))
        print("Conexão FS Track Protocol com servidor " + tracker_ip + " porta " + str(tracker_port))

    except Exception as e:
        print(f"Error connecting to the tracker: {e}")


def calculate_blocks_number(file):
    try:
        file_path = os.path.join(SHARED_FOLDER, file)
        file_size = os.path.getsize(file_path)
        total_blocks = int(file_size / BLOCK_SIZE)
        if file_size % BLOCK_SIZE != 0:
            total_blocks += 1
        return total_blocks
    except Exception as e:
        print(f"ERRO A CALCULAR NUMERO DE BLOCOS DO FICHEIRO: {e}")


def ask_for_file():
    file_name = input("Que ficheiro quer?: ")
    if file_name not in files:
        owners_request = message_types.OwnersRequestMessage(file_name)
        tcp_socket.send(pickle.dumps(owners_request))
        pickle_message = tcp_socket.recv(BUFFER_SIZE)
        try:
            owners_by_block = pickle.loads(pickle_message).owners
        except pickle.UnpicklingError as e:
            print(f"Erro a converter mensagem recebida: {e}")
        else:
            if owners_by_block:
                print("Bloco do ficheiro -> Lista de clientes que o possui:")
                # {IP: [BLOCOS]}
                blocks_by_client: Dict[str, List[int]] = {}
                for block, clients in owners_by_block.items():
                    with blocks_available_lock:
                        if file_name in blocks_available and block in blocks_available[file_name]:
                            continue
                        print(f"{block} -> {clients}")
                        best_client = choose_best_client(clients)
                        if best_client not in blocks_by_client.keys():
                            blocks_by_client.update({best_client: [block]})
                        else:
                            blocks_by_client[best_client].append(block)

                input("Pressionar Enter para comecar transferencia...")
                file_info_request = message_types.FileInfoRequestMessage(file_name)
                tcp_socket.send(pickle.dumps(file_info_request))
                pickle_message = tcp_socket.recv(BUFFER_SIZE)
                try:
                    file_info = pickle.loads(pickle_message)
                except pickle.UnpicklingError as e:
                    print(f"Erro a converter mensagem recebida: {e}")
                else:
                    file_info_path = os.path.join(TEMP_PATH, (file_name + "_info"))
                    with open(file_info_path, 'w') as file:
                        file.write(file_info.file_hash + "\n" + str(file_info.total_blocks))

                for client, blocks in blocks_by_client.items():
                    block_request = message_types.BlockRequestMessage(file_name, blocks)
                    udp_socket.sendto(pickle.dumps(block_request), (client, udp_port))

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
    global EXIT_FLAG
    try:
        disconnect_message = message_types.DisconnectMessage()
        tcp_socket.send(pickle.dumps(disconnect_message))
        tcp_socket.shutdown(socket.SHUT_RDWR)
        tcp_socket.close()
        EXIT_FLAG = True
    except Exception as e:
        print(f"Erro ao desconectar: {e}")


def clear_terminal():
    subprocess.call("clear", shell=True)


def calculate_file_hash(file_path):
    hasher = hashlib.sha256()
    with open(file_path, 'rb') as file:
        for chunk in iter(lambda: file.read(4096), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def calculate_block_hash(block_data):
    hasher = hashlib.sha256()
    hasher.update(block_data)
    return hasher.hexdigest()


if __name__ == "__main__":
    if len(sys.argv) < 3 or len(sys.argv) > 4:
        print("Usage: FS_Node <share_folder> <server_address> <port>")
        sys.exit(1)
    main()
