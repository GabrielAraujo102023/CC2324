import hashlib
import os
import shutil
import subprocess
import socket
import threading
import sys
import re
import time
from typing import Dict
import select
import message_types as msgt
import pickle

# Informa sobre que comando usar para executar a aplicação
if len(sys.argv) < 3 or len(sys.argv) > 4:
    print("Usage: FS_Node <share_folder> <server_name> <port>")
    sys.exit(1)

# Lock para variável global files
files_lock = threading.Lock()
# Caminho para a pasta partilhada
SHARED_FOLDER = sys.argv[1]
# Caminho para a pasta temp da pasta partilhada
TEMP_PATH = os.path.join(SHARED_FOLDER, "temp")


# Retorna nomes de ficheiros presentes na pasta partilhada e limpa blocos corrompidos na pasta temporária
def read_sys_files(folder, startup):
    with files_lock:
        sys_files = []
        for filename in os.listdir(folder):
            file_path = os.path.join(folder, filename)
            # Aqui ignora a pasta temp
            if os.path.isfile(file_path):
                sys_files.append(filename)

    # Limpa blocos potencialmente corrompidos quando antes do cliente se conectar ao servidor
    if startup and os.path.exists(TEMP_PATH):
        for _, _, sys_blocks in os.walk(TEMP_PATH):
            for block in sys_blocks:
                # Ficheiros com o sufixo _temp, têm grande probabilidade de estar corrompidos
                if "temp" in block:
                    os.remove(os.path.join(TEMP_PATH, block))

    return sys_files


# Usado apenas para usar a função clear_terminal
os.environ['TERM'] = 'xterm'
# Nome do servidor passado por argumento
tracker_name = sys.argv[2]
# Porta do servidor
tracker_port = 9090
# Caso a porta do servidor seja passada por argumento, faz a alteração
if len(sys.argv) == 4:
    tracker_port = sys.argv[3]
# Porta a usar para a socket udp
udp_port = 9090
# Socket tcp a usar para a conexão com o servidor
tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
# Socket udp para receber data
udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
# Flag usada para terminar todos os ciclos e, consequentemente threads, antes de encerrar a aplicação
EXIT_FLAG = False
# Tamanho dos blocos lidos de ficheiros
BLOCK_SIZE = 512
# Armazena o nome dos ficheiros na pasta partilhada e limpa os corrompidos da pasta temp
files = read_sys_files(SHARED_FOLDER, True)
# Armazena os blocos disponíveis na pasta temp e se estão a ser usados ou não (ser lidos para tranferir para outro cliente)
# blocks_available: {"filename": {blocknumber: bool}}
blocks_available: Dict[str, Dict[int, bool]] = {}
# Lock para variável global blocks_available
blocks_available_lock = threading.Lock()
# Inicializa variável que guarda o IP do próprio cliente
LOCAL_ADRRESS = ""
# Tamanho do buffer usado para ambas as sockets
BUFFER_SIZE = 16384
# Guarda, temporariamente, os acks que recebe dos blocos que transfere para outros clientes
block_data_acks = {}
# Lock para variável global block_data_acks
block_data_acks_lock = threading.Lock()
# Guarda, temporariamente, os acks que recebe dos pedidos de blocos que envia a outros clientes
block_request_acks = {}
# Lock para variável global block_request_acks
block_request_acks_lock = threading.Lock()
# Guarda, temporariamente, as respostas que vêm do DNS
dns_replies = {}
# Lock para variável global dns_replies
dns_replies_lock = threading.Lock()
# Token de resposta usado quando é pedido ao DNS para resolver uma lista de nomes, para inicar-mos uma transferência
DNS_REPLY_TRANSF_TOKEN = 'transf'
# Timeout usado para espera de resposta a uma mensagem enviada a outro cliente
TIMEOUT = 2
# Máximo de timeouts que podem acontecer até desistir
MAX_TIMEOUTS = 5
MY_NAME = socket.gethostname()
ip_cache = {}
ip_cache_lock = threading.Lock()
transfers = {}
transfers_lock = threading.Lock()
HOST_NAME = ""
tracker_ip = ""
DNS_IP = "10.0.7.10"
DNS_PORT = 9091
tcp_socket_lock = threading.Lock()


def main():
    clear_terminal()
    # Cria e inicia thread que fica à espera de receber mensagens de outros clientes, na socket udp
    data_transfer_thread = threading.Thread(target=data_transfer)
    data_transfer_thread.start()
    udp_socket.bind((LOCAL_ADRRESS, udp_port))
    # Conecta-se ao servidor
    connect_to_tracker()
    first_time = True

    # Menu apresentado ao cliente
    while not EXIT_FLAG:
        if not first_time:
            clear_terminal()
            print_transfer_protocol()
            print_tracker_protocol()
        first_time = False
        print("---------------------------------------------------------")
        print("1 - Transferir ficheiro\n"
              "2 - Consultar transferências\n"
              "3 - Fechar conexão")
        print("---------------------------------------------------------")

        choice = input("Escolha: ")
        if choice == "1":
            clear_terminal()
            print("Transferir ficheiro")
            print("---------------------------------------------------------")
            file_name = input("Nome do ficheiro: ")
            # Procura o ficheiro pedido
            find_file(file_name)
        elif choice == "2":
            if not transfers:
                clear_terminal()
                print("Ainda não realizou transferências!\n")
                input("Pressionar Enter para voltar ao menu...")
            else:
                transfer_menu()
        elif choice == "3":
            # Desconecta do servidor e encerra aplicação
            disconnect()

    # Espera que a socket udp encerre antes de encerrar a aplicação
    data_transfer_thread.join()
    print(f"DESCONETADO COM SUCESSO!")


def transfer_menu():
    choice = ""
    while choice != '2':
        clear_terminal()
        print("Lista de transferências:")
        print("---------------------------------------------------------")
        for file, state in transfers.items():
            if isinstance(state, tuple):
                with blocks_available_lock:
                    blocks_transfered = len(blocks_available[file])
                print(f"-> {file} | Estado: {state[0]} {round(blocks_transfered / state[1] * 100, 2)}%")
            else:
                print(f"-> {file} | Estado: {state}")

        print("\n---------------------------------------------------------")
        print("1 - Atualizar lista\n"
              "2 - Voltar ao menu anterior")
        print("---------------------------------------------------------")

        choice = input("Escolha: ")


def print_transfer_protocol():
    print(f"FS Transfer Protocol: {HOST_NAME} à escuta na porta UDP {udp_port} ")


def print_tracker_protocol():
    print(f"FS Track Protocol: Conectado com {tracker_ip} na porta {tracker_port}")


def data_transfer():
    try:
        global LOCAL_ADRRESS
        LOCAL_ADRRESS, _ = udp_socket.getsockname()
        global HOST_NAME
        HOST_NAME = socket.gethostname()

        print_transfer_protocol()

        while not EXIT_FLAG:
            # Usado para verificar a exit_flag periodicamente
            ready, _, _ = select.select([udp_socket], [], [], 3)
            for _ in ready:
                # Lê mensagem do buffer e o ip de quem a enviou
                pickle_message, peer_ip = udp_socket.recvfrom(BUFFER_SIZE)
                try:
                    # Deserializa a mensagem
                    message = pickle.loads(pickle_message)
                except pickle.UnpicklingError as e:
                    print(f"Erro a converter mensagem recebida: {e}")
                else:
                    if message.type == msgt.MessageType.BLOCK_REQUEST:
                        # Cria thread para processar um pedido de blocos
                        handle_block_request_thread = threading.Thread(target=handle_block_request,
                                                                       args=[message.data_hash, message.file_name,
                                                                             message.blocks, peer_ip])
                        handle_block_request_thread.daemon = True
                        handle_block_request_thread.start()

                    elif message.type == msgt.MessageType.BLOCK_DATA:
                        # Cria thread para processar um bloco recebido
                        receive_block_thread = threading.Thread(target=receive_block, args=[message.block_name,
                                                                                            message.block_data,
                                                                                            message.block_hash,
                                                                                            peer_ip])
                        receive_block_thread.daemon = True
                        receive_block_thread.start()

                    elif message.type == msgt.MessageType.BLOCK_DATA_ACK:
                        # Guarda um block_data_ack que recebeu de um cliente a quem enviou um bloco
                        with block_data_acks_lock:
                            block_data_acks.update({message.block_name: message})
                    elif message.type == msgt.MessageType.BLOCK_REQUEST_ACK:
                        # Guarda um block_request_ack que recebeu de um cliente a quem enviou um pedido de blocos
                        with block_request_acks_lock:
                            block_request_acks.update({message.file_name: message})
                    elif message.type == msgt.MessageType.DNS_REPLY:
                        # Guarda a resposta do DNS que recebeu
                        with dns_replies_lock:
                            dns_replies.update({message.reply_token: message})

    except Exception as e:
        print(f"ERRO NA PORTA UDP: {e}")
    udp_socket.close()
    print(f"PORTA UDP FECHADA")


# Processa um bloco recebido
def receive_block(block_name, block_data, block_hash, sender_ip):
    # Se a hash calculada pelo cliente que recebeu for diferente da que o cliente que enviou, envia ack a dizer que
    # recebeu o bloco mas que este ficou corrompido e termina o processamento. Se forem iguais, envia ack a dizer que
    # recebeu bloco e não foi corrompido e continua o processamento desse bloco
    if calculate_data_hash(block_data) != block_hash:
        block_ack = msgt.BlockDataAckMessage(block_name, True)
        udp_socket.sendto(pickle.dumps(block_ack), sender_ip)
        return
    else:
        block_ack = msgt.BlockDataAckMessage(block_name, False)
        udp_socket.sendto(pickle.dumps(block_ack), sender_ip)

    # Caminho temporário guardar bloco
    save_path_temp = os.path.join(TEMP_PATH, block_name + "temp")
    # Caminho definitivo
    save_path_final = os.path.join(TEMP_PATH, block_name)

    # Escreve data do bloco para um ficheiro
    corrupted = True
    while corrupted:
        with open(save_path_temp, 'wb') as file:
            file.write(block_data)
        # Certifica-se que não há erros a escrever o bloco no ficheiro, visto que o bloco não estava corrompido quando foi recebido
        if calculate_file_hash(save_path_temp) == block_hash:
            corrupted = False

    # Operação atómica. Evita que o bloco seja corrompido ao movê-lo para o caminho final
    shutil.move(save_path_temp, save_path_final)

    file_name, block_number = block_name.split("_")

    with blocks_available_lock:
        # Se já tiver o bloco não atualiza nada (pode acontecer quando o cliente me envia o mesmo bloco devido a ter
        # a ter atingido o timeout de espera de um ack
        # Se já tiver blocos do ficheiro a que o bloco recebido pertence, adiciono a lista de blocos desse ficheiro
        # Se for o primeiro bloco, adiciono esse ficheiro e bloco aos blocos disponíveis
        if file_name in blocks_available and block_number not in blocks_available[file_name]:
            blocks_available[file_name].update({int(block_number): False})
        elif file_name not in blocks_available:
            blocks_available.update({file_name: {int(block_number): False}})

    # Informa servidor que tem o bloco disponível para outros clientes
    update_tracker(block_name)


# Elimina ficheiros da pasta temp correspondentes a um ficheiro que foi montado com sucesso
def delete_temp_blocks(temp_blocks):
    aux = temp_blocks.copy()
    file_name = ""

    while aux:
        for block in temp_blocks:
            file_name, block_number = block.split("_")
            # Função vai correr até eliminar todos os ficheiros. Pode não ser possível eliminar um determinado ficheiro
            # devido a este estar a ser lido para ser enviado para um cliente que o tenha pedido
            if blocks_available[file_name][int(block_number)]:
                continue
            else:
                block_path = os.path.join(TEMP_PATH, block)
                os.remove(block_path)
                aux.remove(block)

    os.remove(os.path.join(TEMP_PATH, file_name + "_info"))


# Fornece uma chave para ordenar uma lista
def sort_key(block):
    block_name, block_number = block.split("_")
    return block_name, int(block_number)


# Monta um ficheiro e retorna ficheiros a serem eliminados da pasta temp
def mount_file(file_name, file_hash):
    file_blocks = []
    blocks = read_sys_files(TEMP_PATH, False)
    # Procura todos os blocos que constituem o ficheiro a montar
    for block in blocks:
        block_name, block_number = block.split("_")
        if block_name != file_name or block_number == "info":
            continue
        else:
            file_blocks.append(block)

    # Ordena os blocos por ordem de escrita
    file_blocks = sorted(file_blocks, key=sort_key)
    # print(f"LISTA DE BLOCOS: {file_blocks}")
    mounted_file_path_temp = os.path.join(SHARED_FOLDER, file_name + "_temp")
    mounted_file_path_final = os.path.join(SHARED_FOLDER, file_name)

    mount_complete = False
    tries = 0
    while not mount_complete and tries < 3:
        tries += 1
        with open(mounted_file_path_temp, 'wb') as mounted_file_temp:
            # Escreve todos os blocos no ficheiro
            for block in file_blocks:
                block_path = os.path.join(TEMP_PATH, block)

                with open(block_path, 'rb') as block_data:
                    block_info = block_data.read()
                    mounted_file_temp.write(block_info)
        # Calcula a hash do ficheiro montado e compara com a do ficheiro original
        # Se forem diferentes, elimina o ficheiro montado e tenta de novo.
        # Visto que os blocos foram bem recebidos (todas as hashs coincidiam), o ficheiro tem que ser bem montado, eventualmente
        if calculate_file_hash(mounted_file_path_temp) != file_hash:
            os.remove(mounted_file_path_temp)
            time.sleep(1)
        else:
            global files
            # print("FICHEIRO BEM MONTADO")
            shutil.move(mounted_file_path_temp, mounted_file_path_final)
            files = read_sys_files(SHARED_FOLDER, False)
            mount_complete = True

    return file_blocks, mount_complete


# Informa o servidor que tem um novo bloco de ficheiro disponível
def update_tracker(block_name):
    with tcp_socket_lock:
        block_update = msgt.BlockUpdateMessage(block_name)
        try:
            tcp_socket.send(pickle.dumps(block_update))
        except Exception as e:
            print(f"ERRO AO ATUALIZAR FICHEIRO NO TRACKER: {e}")


# Processa um pedido de blocos
def handle_block_request(data_hash, file_name, blocks, requester_ip):
    # Calcula a hash da lista de blocos que foi pedida com a hash calculada por quem enviou o pedido
    # Se forem iguais, envia ack a dizer que recebeu o pedido e que não está corrompido e continua o processamento do pedido
    # Se forem diferents, envia ack a dizer que recebeu o pedido e que está corrompido e interrompe o processamento do pedido
    if calculate_data_hash(blocks, True) == data_hash:
        block_request_ack = msgt.BlockRequestAckMessage(file_name, False)
        udp_socket.sendto(pickle.dumps(block_request_ack), requester_ip)
    else:
        block_request_ack = msgt.BlockRequestAckMessage(file_name, True)
        udp_socket.sendto(pickle.dumps(block_request_ack), requester_ip)
        return

    # Possui o ficheiro completo. Abre-o, lê os blocos pedidos e envia-os
    if file_name in files:
        for block in blocks:
            block_name = file_name + "_" + str(block)
            file_path = os.path.join(SHARED_FOLDER, file_name)

            with open(file_path, 'rb') as file:
                file.seek(block * BLOCK_SIZE)
                block_data = file.read(BLOCK_SIZE)

            send_block(block_name, block_data, requester_ip)

    # Não possui o ficheiro completo
    elif file_name in blocks_available:
        with blocks_available_lock:
            # Muda a flag dos blocos para evitar que estes sejam eliminados enquanto os envia
            for block in blocks:
                blocks_available[file_name][block] = True

            # Abre, lê os blocos e envia-os. A cada bloco que envia, volta a mudar a sua flag
            for block in blocks:
                block_name = file_name + "_" + str(block)
                file_path = os.path.join(TEMP_PATH, block_name)

                with open(file_path, 'rb') as file:
                    block_data = file.read()

                blocks_available[file_name][block] = False
                send_block(block_name, block_data, requester_ip)


# Envia uma mensagem com um bloco de um ficheiro a um cliente
def send_block(block_name, block_data, requester_ip):
    # Calcula a hash do bloco a enviar e constrói a mensagem
    block_hash = calculate_data_hash(block_data)
    block_message = msgt.BlockDataMessage(block_name, block_data, block_hash)

    try:
        block_corrupted = True
        while block_corrupted:
            udp_socket.sendto(pickle.dumps(block_message), requester_ip)
            start_time = time.time()
            timeout_count = 0
            # Depois de enviar a mensagem fica à espera de um ack do cliente
            # Se receber um ack a dizer que bloco foi corrompido, envia o bloco de novo
            # Se receber um ack a dizer que o bloco não foi corrompido, termina o envio
            # Se ocorrer um timeout enquanto espera pelo ack, envia o bloco de novo
            # Se ultrapassar o número máximo de timeouts, desiste de enviar o bloco
            while True:
                if time.time() - start_time > TIMEOUT or timeout_count >= MAX_TIMEOUTS:
                    timeout_count += 1
                    break

                if block_name not in block_data_acks:
                    continue
                else:
                    # O ack é eliminado depois de ser processado
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


# Conecta-se ao servidor
def connect_to_tracker():
    global tracker_ip
    # Pede ao DNS o IP do tracker
    tracker_ip = get_ips_from_dns([tracker_name])
    if len(tracker_ip) == 0:
        print("Não foi possível estabelecer uma conexão ao tracker.")
        sys.exit(1)
    tracker_ip = tracker_ip[0]
    try:
        # Cria conexão tcp com o servidor
        tcp_socket.connect((tracker_ip, int(tracker_port)))
        print_tracker_protocol()

        files_info = []
        blocks_info = []

        # Calcula a hash e o número total de blocos de um ficheiro e adiciona-os ao files_info juntamente com o nome do mesmo
        for file in files:
            file_hash = calculate_file_hash(os.path.join(SHARED_FOLDER, file))
            total_blocks = calculate_blocks_number(file)
            files_info.append((file, total_blocks, file_hash))

        if os.path.exists(TEMP_PATH):
            for _, _, blocks in os.walk(TEMP_PATH):
                # Por cada bloco na pasta temp, adiciona o nome do bloco, a hash e número total de blocos do ficheiro a
                # que pertence ao blocks_info. Também adiciona os nomes dos blocos ao blocks_available.
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

                    blocks_info.append((block, file_hash, int(total_blocks)))

                    with blocks_available_lock:
                        if file_name not in blocks_available:
                            blocks_available.update({file_name: {block_number: False}})
                        else:
                            blocks_available[file_name].update({block_number: False})
        else:
            os.makedirs(TEMP_PATH)

        # Informa o servidor dos blocos e ficheiros que quer partilhar
        with tcp_socket_lock:
            new_connection = msgt.NewConnectionMessage(files_info, blocks_info, MY_NAME)
            tcp_socket.send(pickle.dumps(new_connection))

    except Exception as e:
        print(f"Error connecting to the tracker: {e}")
        global EXIT_FLAG
        EXIT_FLAG = True


# Calcula o numero total de blocos de um ficheiro
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


# Procura o ficheiro pedido
def find_file(file_name):
    if file_name not in files:
        # Envia mensagem ao servidor com pedido de donos de blocos do ficheiro pedido
        with tcp_socket_lock:
            owners_request = msgt.OwnersRequestMessage(file_name)
            tcp_socket.send(pickle.dumps(owners_request))
            # Recebe a resposta
            pickle_message = tcp_socket.recv(BUFFER_SIZE)
        try:
            # Deserializa a mensagem
            message = pickle.loads(pickle_message)
            blocks_by_owner = message.owners
        except pickle.UnpicklingError as e:
            print(f"Erro a converter mensagem recebida: {e}")
        else:
            # Se o ficheiro estiver disponível na rede, recebe um dicionário do tipo {owner: [blocks]}
            if blocks_by_owner:
                # Reúne informação sobre o ficheiro
                gather_information(file_name, blocks_by_owner, message.total_blocks)
            else:
                print("\nFicheiro não encontrado")
                input("Pressionar Enter para voltar ao menu...")

    else:
        print("\nJá possui o ficheiro pedido")
        input("Pressionar Enter para voltar ao menu...")


def gather_information(file_name, blocks_by_owner_name, total_blocks):
    values = list(blocks_by_owner_name.values())
    # Pede ao DNS o IP de todos os nomes a quem quer pedir blocos
    ips = get_ips_from_dns(list(blocks_by_owner_name.keys()))
    # print("IPS = " + str(ips))
    if len(ips) == 0:
        print("Não foi possível resolver nomes para pedir ficheiro")
        sys.exit(1)

    # Transforma um dicionário de Nome -> Blocos em IPs -> Blocos
    blocks_by_owner = {ips[i]: values[i] for i in range(len(ips))}

    print("\nA recolher informação sobre os peers. Aguarde...")
    # Calcula velocidade de ligação de cada owner
    latency_by_owner = {owner: get_latency(owner) for owner in blocks_by_owner}
    # Ordena o dicinário por velocidade de ligação do owner
    blocks_by_owner_sorted = dict(sorted(blocks_by_owner.items(), key=lambda item: latency_by_owner[item[0]]))
    print("Transferencia vai começar dentro de 3 segundos...")
    time.sleep(3)
    # Cria thread para processar transferência
    transfer_thread = threading.Thread(target=transfer_file, args=[file_name, blocks_by_owner_sorted, latency_by_owner])
    transfer_thread.daemon = True
    transfer_thread.start()
    with transfers_lock:
        transfers[file_name] = ("Em curso", total_blocks)
    input("Transferencia iniciada. Pressionar Enter para voltar ao menu...")


def transfer_file(file_name, blocks_by_owner_sorted, latency_by_owner):
    # VERIFICO SE JÁ TENHO INFORMAÇÃO SOBRE O FICHEIRO QUE VOU TRANSFERIR
    # SE JÁ TIVER, TENHO QUE VER QUE BLOCOS É QUE JÁ TENHO PARA NAO OS PEDIR OUTRA VEZ
    # SE NÃO TIVER, PEÇO INFORMAÇÃO SOBRE O FICHEIRO

    blocks_owned = []
    total_blocks = []
    file_hash = ""
    global files

    # Se já tem informação sobre o ficheiro que pediu, verifica que blocos já possui para não os voltar a pedir
    if os.path.exists(os.path.join(TEMP_PATH, file_name + "_info")):
        with open(os.path.join(TEMP_PATH, file_name + "_info"), 'r') as file_info:
            file_hash = file_info.readline().replace('\n', '')
            total_blocks = list(range(int(file_info.readline())))

        with blocks_available_lock:
            if file_name in blocks_available:
                blocks_owned = list(blocks_available[file_name].keys())

    # Se não, pede informação sobre o ficheiro ao servidor
    else:
        with tcp_socket_lock:
            file_info_request = msgt.FileInfoRequestMessage(file_name)
            tcp_socket.send(pickle.dumps(file_info_request))
            pickle_message = tcp_socket.recv(BUFFER_SIZE)
        try:
            file_info = pickle.loads(pickle_message)
        except pickle.UnpicklingError as e:
            print(f"Erro a converter mensagem recebida: {e}")
        # Guarda um ficheiro no temp com informações sobre o ficheiro pedido
        else:
            # Preenche total_blocks com números correspondentes ao total de blocos do ficheiro pedido
            total_blocks = list(range(file_info.total_blocks))
            file_hash = file_info.file_hash
            file_info_temp = os.path.join(TEMP_PATH, file_name + "_infotemp")
            file_info_final = os.path.join(TEMP_PATH, file_name + "_info")

            with open(file_info_temp, 'w') as file:
                file.write(file_info.file_hash + "\n" + str(file_info.total_blocks))

            shutil.move(file_info_temp, file_info_final)

    # Cria uma lista de blocos que vai pedir retirando os que já possui
    blocks_needed = [block for block in total_blocks if block not in blocks_owned]
    # print("BLOCKS NEEDED")
    # print(blocks_needed)
    # print("BLOCKS TO REQUEST")
    # print(blocks_to_request)
    # Soma os tempos de resposta de todos os owners
    total_latency = sum(latency_by_owner.values())
    # print(f"total latency = {total_latency}")
    # Lista de owners
    owners = list(blocks_by_owner_sorted.keys())
    # print("OWNERS")
    # print(owners)
    max_blocks_to_request = 5
    missing_block = False

    # Enquanto houver blocos necessários, tenta pedir esses blocos
    while blocks_needed:
        # Se não tiver mais clientes a quem pedir um bloco qualquer, para a transferência
        for block in blocks_needed:
            if not any(block in blocks for blocks in blocks_by_owner_sorted.values()):
                missing_block = True
                break
        if missing_block:
            with transfers_lock:
                transfers.update({file_name: "Interrompida"})
            break

        # Verificar se o ficheiro ainda está disponível na rede
        with tcp_socket_lock:
            file_state_request = msgt.FileStateRequestMessage(file_name)
            tcp_socket.send(pickle.dumps(file_state_request))
            # Recebe a resposta
            pickle_message = tcp_socket.recv(BUFFER_SIZE)

        try:
            # Deserializa a mensagem
            file_state = pickle.loads(pickle_message)
        except pickle.UnpicklingError as e:
            print(f"Erro a converter mensagem recebida: {e}")
        else:
            if not file_state.available:
                with transfers_lock:
                    transfers.update({file_name: "Interrompida"})
                break

        blocks_to_request = blocks_needed.copy()

        # Distribui pedidos dos blocos a pedir pelos vários owners enquanto existirem blocos a pedir
        while blocks_to_request:
            bad_owner_list = []
            if not owners:
                break
            for owner in owners:
                # Calcula que blocos é que precisa de pedir a um determinado owner
                blocks = blocks_by_owner_sorted[owner]
                blocks_to_request_from_owner = list(set(blocks) & set(blocks_to_request))

                if blocks_to_request_from_owner:
                    # Calcula a proporção dos blocos a pedir a um owner baseado na sua latência em relação à soma de todas as latências
                    owner_latency = latency_by_owner[owner]
                    proportion_of_blocks = (total_latency - owner_latency) / total_latency
                    # Número de blocos a pedir ao owner
                    # O max_blocks_to_request evita que se peçam todos os blocos a um cliente que seja muito mais rápido
                    # que os outros e que tenha todos os blocos do ficheiro
                    num_blocks_to_request = max(1, min(int(proportion_of_blocks *
                                                           len(blocks_to_request_from_owner)), max_blocks_to_request))
                    # Blocos que vai, efetivamente, pedir
                    blocks_to_request_from_owner = blocks_to_request_from_owner[:num_blocks_to_request]

                    # Pede os blocos e remove-os da lista do owner para não voltar a pedir ao mesmo caso este pedido falhe
                    success = send_block_request(file_name, blocks_to_request_from_owner, owner)
                    blocks_by_owner_sorted[owner] = [block for block in blocks if
                                                     block not in blocks_to_request_from_owner]

                    # Se o pedido tiver sucesso, remove os blocos pedidos dos blocos a pedir
                    if success:
                        blocks_to_request = [block for block in blocks_to_request if
                                             block not in blocks_to_request_from_owner]
                    else:
                        bad_owner_list.append(owner)

            if bad_owner_list:
                for bad_owner in bad_owner_list:
                    owners.remove(bad_owner)
                    del blocks_by_owner_sorted[bad_owner]
                    total_latency -= latency_by_owner[bad_owner]
                    del latency_by_owner[bad_owner]

        if not owners:
            continue

        # print("PEDI OS BLOCOS TODOS PELA PRIMEIRA VEZ")
        tries = 0

        # Verifica blocos que já recebeu.
        # Se ultrapassar o max_timeouts, volta a pedir os blocos necessários a owners diferentes
        while tries < MAX_TIMEOUTS:
            time.sleep(TIMEOUT + 1)
            if blocks_available[file_name]:
                # Lista de blocos que já recebu
                blocks_received = list(blocks_available[file_name].keys())
                # print(f"BLOCKS_RECEIVED: {blocks_received}")
                # Remove dos blocos necessários os que já recebeu
                blocks_needed = [block for block in blocks_needed if block not in blocks_received]
                # print(f"BLOCKS_NEEDED: {blocks_needed}")
            # Se ainda não recebeu tudo o que pediu, continua à espera (para contar com os timeouts do send_block())
            if blocks_needed:
                tries += 1
            else:
                # Quando recebe todos os blocos, monta o ficheiro
                temp_blocks, mount_complete = mount_file(file_name, file_hash)
                # Atualiza files para incluir o ficheiro acabado de montar
                if mount_complete:
                    with transfers_lock:
                        transfers.update({file_name: "Terminada"})
                else:
                    with transfers_lock:
                        transfers.update({file_name: "Corrompida"})
                # Elimina blocos temporários
                delete_temp_blocks(temp_blocks)

                break


# Envia uma mensagem com um pedido de blocos e retorna true se receber
def send_block_request(file_name, blocks, owner_ip):
    # Calcula uma hash dos blocos pedidos e constrói a mensagem
    data_hash = calculate_data_hash(blocks, True)
    block_request_message = msgt.BlockRequestMessage(file_name, blocks, data_hash)
    timeouts_count = 0

    try:
        data_corrupted = True
        while data_corrupted:
            # Envia mensagem
            udp_socket.sendto(pickle.dumps(block_request_message), (owner_ip, udp_port))
            start_time = time.time()

            # Espera pelo ack do outro cliente
            while True:
                # Espera até um máximo de timeouts
                if time.time() - start_time > TIMEOUT:
                    timeouts_count += 1
                    if timeouts_count >= MAX_TIMEOUTS:
                        return False
                    break

                if file_name not in block_request_acks:
                    continue
                else:
                    # Se recebeu o ack e data não foi corrompida, elimina o ack e retorna true
                    if not block_request_acks[file_name].corrupted:
                        data_corrupted = False
                        with block_request_acks_lock:
                            del block_request_acks[file_name]
                        break
                    # Se recebeu o ack e data foi corrompida, elimina ack e envia o pedido de novo
                    else:
                        with block_request_acks_lock:
                            del block_request_acks[file_name]
                        break

    except Exception as e:
        print(f"ERRO AO ENVIAR PEDIDO: {e}")

    return True


# Utiliza pings para verificar velocidade de conexão
def get_latency(ip):
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


# Desconeta-se do servidor e muda a exit_flag para terminar a aplicação
def disconnect():
    global EXIT_FLAG
    try:
        # Envia mensagem ao servidor a dizer que se vai desconectar
        with tcp_socket_lock:
            disconnect_message = msgt.DisconnectMessage()
            tcp_socket.send(pickle.dumps(disconnect_message))
        # Fecha socket tcp
        tcp_socket.shutdown(socket.SHUT_RDWR)
        tcp_socket.close()
        EXIT_FLAG = True
    except Exception as e:
        print(f"Erro ao desconectar: {e}")


# Limpa o terminal
def clear_terminal():
    subprocess.call("clear", shell=True)


# Calcula a hash de um ficheiro (caminho para o ficheiro) usando sha256
def calculate_file_hash(file_path):
    hasher = hashlib.sha256()
    with open(file_path, 'rb') as file:
        for chunk in iter(lambda: file.read(4096), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


# Calcula a hash de um bloco de data ou de uma lista de blocos usando sha256
def calculate_data_hash(block_data, is_blocks=False):
    if is_blocks:
        # Usado para transformar os números dos blocos em bytes
        block_bytes = b''.join(int.to_bytes(i, length=8, byteorder='big') for i in block_data)
        block_data = block_bytes
    hasher = hashlib.sha256()
    hasher.update(block_data)
    return hasher.hexdigest()


# Pede ao DNS para traduzir uma lista de nomes em lista de IPs
def get_ips_from_dns(requested_ips):
    reply_token: str
    if len(requested_ips) == 1:
        reply_token = requested_ips[0]
    else:
        reply_token = DNS_REPLY_TRANSF_TOKEN

    # Envia a mensagem
    contact_dns(MY_NAME, requested_ips, reply_token)
    ips = []
    start_time = time.time()
    timeout_counter = 0
    # Espera por uma resposta durante um certo tempo, podendo no máximo atinger 3 timeouts
    while timeout_counter < MAX_TIMEOUTS:
        if time.time() - start_time > TIMEOUT:
            timeout_counter = timeout_counter + 1
            start_time = time.time()
            continue
        # Verifica se existe uma resposta na lista de respostas com o token respondente à mensagem que enviou
        if reply_token not in dns_replies:
            continue
        ips = dns_replies[reply_token].ips
        with dns_replies_lock:
            del dns_replies[reply_token]
        break
    if timeout_counter < 3:
        return ips
    print("DNS TIMEOUT ERROR")
    choice = ''
    # Caso atinja os 3 timeouts, pergunta ao utilizador se quer tentar  fazer o pedido novamente.
    while choice != 'n':
        choice = input("Deseja tentar outra vez? (y/n)")
        if choice == 'y':
            return get_ips_from_dns(requested_ips)
    return []


def contact_dns(sender_name, requested_names, reply_token, delete=True):
    dns_message = msgt.DnsRequest(sender_name, requested_names, reply_token, delete)
    udp_socket.sendto(pickle.dumps(dns_message), (DNS_IP, DNS_PORT))


if __name__ == "__main__":
    main()
