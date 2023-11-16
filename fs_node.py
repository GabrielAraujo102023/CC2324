import hashlib
import os
import shutil
import subprocess
import sys
import socket
import threading
import re
import time
from typing import Dict
import select
import message_types
import pickle
from dns import contact_dns

if len(sys.argv) < 3 or len(sys.argv) > 4:
    print("Usage: FS_Node <share_folder> <server_name> <port>")
    sys.exit(1)

files_lock = threading.Lock()
SHARED_FOLDER = sys.argv[1]
TEMP_PATH = os.path.join(SHARED_FOLDER, "temp")


def read_sys_files(folder, startup):
    with files_lock:
        sys_files = []
        for filename in os.listdir(folder):
            file_path = os.path.join(folder, filename)
            if os.path.isfile(file_path):
                sys_files.append(filename)

    # LIMPA BLOCOS POTENCIALMENTE CORROMPIDOS
    if startup and os.path.exists(TEMP_PATH):
        for _, _, sys_blocks in os.walk(TEMP_PATH):
            for block in sys_blocks:
                if "temp" in block:
                    os.remove(os.path.join(TEMP_PATH, block))

    return sys_files


os.environ['TERM'] = 'xterm'
SHARED_FOLDER = sys.argv[1]
tracker_name = sys.argv[2]
tracker_port = 9090
if len(sys.argv) == 4:
    tracker_port = sys.argv[3]
udp_port = 9090
tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
EXIT_FLAG = False
BLOCK_SIZE = 512
files = read_sys_files(SHARED_FOLDER, True)
files_lock = threading.Lock()
# blocks_available: {"filename": {blocknumber: bool}}
blocks_available: Dict[str, Dict[int, bool]] = {}
blocks_available_lock = threading.Lock()
LOCAL_ADRRESS = ""
BUFFER_SIZE = 1024
block_data_acks = {}
block_data_acks_lock = threading.Lock()
block_request_acks = {}
block_request_acks_lock = threading.Lock()
dns_replies = {}
dns_replies_lock = threading.Lock()
DNS_REPLY_TRANSF_TOKEN = 'transf'
TIMEOUT = 2
MAX_TIMEOUTS = 3
MY_NAME = socket.gethostname()


def main():
    data_transfer_thread = threading.Thread(target=data_transfer)
    data_transfer_thread.start()
    udp_socket.bind((LOCAL_ADRRESS, udp_port))
    connect_to_tracker()

    while not EXIT_FLAG:
        # clear_terminal()
        print("1 - Transferir ficheiro\n"
              "2 - Fechar conexão\n"
              "Escolha:")
        choice = input()
        if choice == "1":
            file_name = input("Que ficheiro quer?: ")
            find_file(file_name)
        elif choice == "2":
            disconnect()

    data_transfer_thread.join()
    print(f"DESCONETADO COM SUCESSO!")


def data_transfer():
    global LOCAL_ADRRESS
    try:
        LOCAL_ADRRESS, _ = udp_socket.getsockname()
        print(f"FS Transfer Protocol: à escuta em {LOCAL_ADRRESS} na porta UDP {udp_port} ")

        while not EXIT_FLAG:
            ready, _, _ = select.select([udp_socket], [], [], 3)
            for _ in ready:
                pickle_message, peer_ip = udp_socket.recvfrom(BUFFER_SIZE)
                try:
                    message = pickle.loads(pickle_message)
                except pickle.UnpicklingError as e:
                    print(f"Erro a converter mensagem recebida: {e}")
                else:
                    # print("MESSAGE -> " + str(message))
                    if message.type == message_types.MessageType.BLOCK_REQUEST:
                        threading.Thread(target=get_ips_to_handle_request, args=[message]).start()
                    elif message.type == message_types.MessageType.BLOCK_DATA:
                        threading.Thread(target=get_ips_tp_receive_block, args=[message]).start()
                    elif message.type == message_types.MessageType.BLOCK_DATA_ACK:
                        with block_data_acks_lock:
                            block_data_acks.update({message.block_name: message})
                    elif message.type == message_types.MessageType.BLOCK_REQUEST_ACK:
                        with block_request_acks_lock:
                            block_request_acks.update({message.file_name: message})
                    elif message.type == message_types.MessageType.DNS_REPLY:
                        with dns_replies_lock:
                            dns_replies.update({message.reply_token: message})

    except Exception as e:
        print(f"ERRO NA PORTA UDP: {e}")
    udp_socket.close()
    print(f"PORTA UDP FECHADA")


def get_ips_to_handle_request(message):
    print(f"RECEBI UMA MENSAGEM COM UM PEDIDO DE FICHEIRO")
    ips = get_ips_from_dns([message.peer_name])
    print("JA FIZ O PEDIDO DOS IPS")
    handle_block_request(message.data_hash, message.file_name, message.blocks, (ips[0], udp_port))


def get_ips_tp_receive_block(message):
    ips = get_ips_from_dns([message.peer_name])
    # print(f"RECEBI UMA MENSAGEM COM UM FICHEIRO")
    receive_block(message.block_name, message.block_data, message.block_hash, (ips[0], udp_port))


def receive_block(block_name, block_data, block_hash, sender_ip):

    if calculate_data_hash(block_data) != block_hash:
        block_ack = message_types.BlockDataAckMessage(block_name, True)
        udp_socket.sendto(pickle.dumps(block_ack), sender_ip)
        print("O BLOCO RECEBIDO É DIFERENTE DO ENVIADO MY BOY")
        return
    else:
        print("RECEBI BEM OS BYTES DO BLOCO. AS HASHS SAO IGUAIS")
        block_ack = message_types.BlockDataAckMessage(block_name, False)
        udp_socket.sendto(pickle.dumps(block_ack), sender_ip)

    save_path_temp = os.path.join(TEMP_PATH, block_name + "temp")
    save_path_final = os.path.join(TEMP_PATH, block_name)
    global files

    corrupted = True
    while corrupted:
        with open(save_path_temp, 'wb') as file:
            file.write(block_data)
        if calculate_file_hash(save_path_temp) == block_hash:
            corrupted = False

    # EVITA QUE O BLOCO SEJA CORROMPIDO AO MOVÊ-LO PARA O CAMINHO FINAL POIS É UMA OPERAÇÃO ATÓMICA
    shutil.move(save_path_temp, save_path_final)

    file_name, block_number = block_name.split("_")

    with blocks_available_lock:
        # SE JÁ TIVER O BLOCO, NAO FAÇO NADA COM ELE
        # (CASO O GAJO ME MANDE O MESMO BLOCO MAIS QUE UMA VEZ POR CAUSA DO TIMEOUT ACONTECER)
        if file_name in blocks_available and block_number not in blocks_available[file_name]:
            blocks_available[file_name].update({int(block_number): False})
        elif file_name not in blocks_available:
            blocks_available.update({file_name: {int(block_number): False}})

    update_tracker(block_name)


def delete_temp_blocks(temp_blocks):
    aux = temp_blocks.copy()
    file_name = ""

    while aux:
        for block in temp_blocks:
            file_name, block_number = block.split("_")
            if blocks_available[file_name][int(block_number)]:
                continue
            else:
                block_path = os.path.join(TEMP_PATH, block)
                os.remove(block_path)
                aux.remove(block)

    os.remove(os.path.join(TEMP_PATH, file_name + "_info"))


def sort_key(block):
    block_name, block_number = block.split("_")
    return block_name, int(block_number)


def mount_file(file_name, file_hash):
    file_blocks = []
    blocks = read_sys_files(TEMP_PATH, False)

    for block in blocks:
        block_name, block_number = block.split("_")
        if block_name != file_name or block_number == "info":
            continue
        else:
            file_blocks.append(block)

    file_blocks = sorted(file_blocks, key=sort_key)
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


def update_tracker(block_name):
    block_update = message_types.BlockUpdateMessage(block_name)

    try:
        tcp_socket.send(pickle.dumps(block_update))
    except Exception as e:
        print(f"ERRO AO ATUALIZAR FICHEIRO NO TRACKER: {e}")


def handle_block_request(data_hash, file_name, blocks, requester_ip):
    print("RECEBIRECEBIRECEBIRECEBI")
    if calculate_data_hash(bytes(blocks)) == data_hash:
        block_request_ack = message_types.BlockRequestAckMessage(file_name, False)
        udp_socket.sendto(pickle.dumps(block_request_ack), requester_ip)
    else:
        block_request_ack = message_types.BlockRequestAckMessage(file_name, True)
        udp_socket.sendto(pickle.dumps(block_request_ack), requester_ip)
        return

    if file_name in files:
        for block in blocks:
            block_name = file_name + "_" + str(block)
            file_path = os.path.join(SHARED_FOLDER, file_name)

            with open(file_path, 'rb') as file:
                file.seek(block * BLOCK_SIZE)
                block_data = file.read(BLOCK_SIZE)

            send_block(block_name, block_data, requester_ip)

    elif file_name in blocks_available:
        with blocks_available_lock:
            for block in blocks:
                blocks_available[file_name][block] = True

            for block in blocks:
                block_name = file_name + "_" + str(block)
                file_path = os.path.join(TEMP_PATH, block_name)

                with open(file_path, 'rb') as file:
                    block_data = file.read()

                blocks_available[file_name][block] = False
                send_block(block_name, block_data, requester_ip)


def send_block(block_name, block_data, requester_ip):
    block_hash = calculate_data_hash(block_data)
    block_message = message_types.BlockDataMessage(block_name, block_data, block_hash, MY_NAME)

    try:
        block_corrupted = True
        while block_corrupted:
            udp_socket.sendto(pickle.dumps(block_message), requester_ip)
            start_time = time.time()

            while True:
                if time.time() - start_time > TIMEOUT:
                    break

                if block_name not in block_data_acks:
                    continue
                else:
                    if not block_data_acks[block_name].corrupted:
                        block_corrupted = False
                        with block_data_acks_lock:
                            del block_data_acks[block_name]
                        break
                    else:
                        with block_data_acks_lock:
                            del block_data_acks[block_name]
                        break

    except Exception as e:
        print(f"ERRO AO ENVIAR BLOCO: {e}")


def connect_to_tracker():
    tracker_ip = get_ips_from_dns([tracker_name])[0]
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

        new_connection = message_types.NewConnectionMessage(files_info, blocks_info, MY_NAME)
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


def find_file(file_name):
    if file_name not in files:
        owners_request = message_types.OwnersRequestMessage(file_name)
        tcp_socket.send(pickle.dumps(owners_request))
        pickle_message = tcp_socket.recv(BUFFER_SIZE)
        try:
            blocks_by_owner = pickle.loads(pickle_message).owners
        except pickle.UnpicklingError as e:
            print(f"Erro a converter mensagem recebida: {e}")
        else:
            if blocks_by_owner:
                transfer_thread = threading.Thread(target=transfer_file, args=[file_name, blocks_by_owner])
                transfer_thread.daemon = True
                transfer_thread.start()
            else:
                print("Ficheiro nao encontrado")
                input("Pressionar Enter para voltar ao menu...")

    else:
        print("Ja possui o ficheiro pedido")
        input("Pressionar Enter para voltar ao menu...")


def transfer_file(file_name, blocks_by_owner_name):
    # Traduz nomes em IPs
    values = list(blocks_by_owner_name.values())
    print("ANTES DE FALAR COM DNS")
    print("LIST IS " + str(list(blocks_by_owner_name.keys())))
    ips = get_ips_from_dns(list(blocks_by_owner_name.keys()))
    print("IPS = " + str(ips))
    blocks_by_owner = {ips[i]: values[i] for i in range(len(ips))}

    # LISTA DE OWNERS ORDENADA POR VELOCIDADE DE LIGAÇÃO
    print("A recolher informação sobre o ficheiro. Aguarde...")
    latency_by_owner = {owner: get_latency(owner) for owner in blocks_by_owner}
    print("JA RECOLHI A INFROMADSADSASDAWQEEWQWEQWQEQFDFDDFSSDF")
    blocks_by_owner_sorted = dict(sorted(blocks_by_owner.items(), key=lambda item: latency_by_owner[item[0]]))
    print("JA DEI SORT A LISTA ADSADSADSAWQEEWQQWFSVXC")

    print("Transferencia vai começar dentro de 3 segundos...")
    time.sleep(3)
    print("Transferencia iniciada")

    # VERIFICO SE JÁ TENHO INFORMAÇÃO SOBRE O FICHEIRO QUE VOU TRANSFERIR
    # SE JÁ TIVER, TENHO QUE VER QUE BLOCOS É QUE JÁ TENHO PARA NOS OS PEDIR OUTRA VEZ
    # SE NÃO TIVER, PEÇO INFORMAÇÃO SOBRE O FICHEIRO

    blocks_owned = []
    total_blocks = []
    file_hash = ""
    global files
    if os.path.exists(os.path.join(TEMP_PATH, file_name + "_info")):
        with blocks_available_lock:
            blocks_owned = list(blocks_available[file_name].keys())
    else:
        file_info_request = message_types.FileInfoRequestMessage(file_name)
        tcp_socket.send(pickle.dumps(file_info_request))
        pickle_message = tcp_socket.recv(BUFFER_SIZE)
        try:
            file_info = pickle.loads(pickle_message)
        except pickle.UnpicklingError as e:
            print(f"Erro a converter mensagem recebida: {e}")
        else:
            total_blocks = list(range(file_info.total_blocks))
            file_hash = file_info.file_hash
            file_info_temp = os.path.join(TEMP_PATH, file_name + "_infotemp")
            file_info_final = os.path.join(TEMP_PATH, file_name + "_info")

            with open(file_info_temp, 'w') as file:
                file.write(file_info.file_hash + "\n" + str(file_info.total_blocks))

            shutil.move(file_info_temp, file_info_final)
    print("0000000000000000000000000000")
    # RETIRO DA LISTA OS BLOCOS QUE JÁ TENHO
    blocks_needed = [block for block in total_blocks if block not in blocks_owned]
    print("BLOCKS NEEDED")
    print(blocks_needed)
    blocks_to_request = blocks_needed.copy()
    print("BLOCKS TO REQUEST")
    print(blocks_to_request)
    total_latency = sum(latency_by_owner.values())
    print(f"total latency = {total_latency}")
    owners = list(blocks_by_owner_sorted.keys())
    print("OWNERS")
    print(owners)
    max_blocks_to_request = 5
    missing_block = False

    while blocks_needed and not missing_block:
        for block in blocks_needed:
            if not any(block in blocks for blocks in blocks_by_owner.values()):
                missing_block = True
                print("FALHEI LOGO NO PRIMEIRO IF")
                break
        if missing_block:
            print("FICHEIRO COMPLETO INDISPONIVEL NA REDE")
            break

        # DISTRIBUI PEDIDOS DOS BLOCOS NECESSÁRIOS PELOS VÁRIOS OWNERS ATÉ TER PEDIDO TODOS OS BLOCOS
        while blocks_to_request:
            # for owner, blocks in blocks_by_owner_sorted.items():
            print("ENTREI NO WHILE BLOCKS_TO_REQUEST")
            for owner in owners:
                blocks = blocks_by_owner_sorted[owner]
                blocks_to_request_from_owner = list(set(blocks) & set(blocks_to_request))

                if blocks_to_request_from_owner:
                    owner_latency = latency_by_owner[owner]
                    proportion_of_blocks = (total_latency - owner_latency) / total_latency
                    # O MAX EVITA QUE SE PEÇAM OS BLOCOS TODOS A UM MUITO MAIS RÁPIDO QUE TENHA TODOS OS BLOCOS
                    num_blocks_to_request = max(1, min(int(proportion_of_blocks *
                                                           len(blocks_to_request_from_owner)), max_blocks_to_request))
                    blocks_to_request_from_owner = blocks_to_request_from_owner[:num_blocks_to_request]

                    success = send_block_request(file_name, blocks_to_request_from_owner, owner)
                    blocks_by_owner[owner] = [block for block in blocks if block not in blocks_to_request_from_owner]

                    if success:
                        blocks_to_request = [block for block in blocks_to_request if
                                             block not in blocks_to_request_from_owner]
                        print("ELE RECEBEU O PEDIDO")

        print("PEDI OS BLOCOS TODOS PELA PRIMEIRA VEZ")
        tries = 0
        while tries < MAX_TIMEOUTS:
            print("ESTOU NO WHILE DAS TRIES")
            time.sleep(TIMEOUT + 1)
            if blocks_available[file_name]:
                blocks_received = list(blocks_available[file_name].keys())
                print(f"BLOCKS_RECEIVED: {blocks_received}")
                blocks_needed = [block for block in blocks_needed if block not in blocks_received]
                print(f"BLOCKS_NEEDED: {blocks_needed}")
            if blocks_needed:
                tries += 1
            else:
                temp_blocks = mount_file(file_name, file_hash)
                files = read_sys_files(SHARED_FOLDER, False)
                delete_temp_blocks(temp_blocks)
                break

    print("TRANSFERENCIA FOI UM SUCESSO")


def send_block_request(file_name, blocks, owner_ip):
    data_hash = calculate_data_hash(bytes(blocks))
    block_request_message = message_types.BlockRequestMessage(file_name, blocks, data_hash, MY_NAME)
    timeouts_count = 0

    try:
        data_corrupted = True
        while data_corrupted:
            print("OWNER IP IS " + owner_ip)
            udp_socket.sendto(pickle.dumps(block_request_message), (owner_ip, udp_port))
            start_time = time.time()

            while True:
                if time.time() - start_time > TIMEOUT:
                    timeouts_count += 1
                    if timeouts_count >= MAX_TIMEOUTS:
                        print("ATINGIU O MAX DE TIMEOUTS")
                        return False
                    break

                if file_name not in block_request_acks:
                    continue
                else:
                    if not block_request_acks[file_name].corrupted:
                        data_corrupted = False
                        with block_request_acks_lock:
                            del block_request_acks[file_name]
                        break
                    else:
                        with block_request_acks_lock:
                            del block_request_acks[file_name]
                        break

    except Exception as e:
        print(f"ERRO AO ENVIAR PEDIDO: {e}")

    return True


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


def calculate_data_hash(block_data):
    hasher = hashlib.sha256()
    hasher.update(block_data)
    return hasher.hexdigest()


def get_ips_from_dns(requested_ips):
    reply_token: str
    if len(requested_ips) == 1:
        reply_token = requested_ips[0]
    else:
        reply_token = DNS_REPLY_TRANSF_TOKEN

    contact_dns(MY_NAME, udp_socket, requested_ips, reply_token)
    ips = []
    while True:
        if reply_token not in dns_replies:
            continue
        ips = dns_replies[reply_token].ips
        with dns_replies_lock:
            del dns_replies[reply_token]
        break
    return ips


if __name__ == "__main__":
    main()
