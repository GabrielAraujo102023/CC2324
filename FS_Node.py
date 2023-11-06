import base64
import os
import subprocess
import sys
import socket
import threading
import json

import select

if len(sys.argv) < 3 or len(sys.argv) > 4:
    print("Usage: FS_Node <share_folder> <server_address> <port>")
    sys.exit(1)

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
files_available = []
FILE_REQUEST = 1
FILE = 2
local_address = ""


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
                if message["type"] == FILE_REQUEST:
                    # print(f"RECEBI UMA MENSAGEM COM UM PEDIDO DE FICHEIRO")
                    t = threading.Thread(target=send_file, args=[message["filename"], addr])
                    t.start()
                elif message["type"] == FILE:
                    # print(f"RECEBI UMA MENSAGEM COM UM FICHEIRO")
                    t = threading.Thread(target=receive_file, args=[message["filename"], message["base64_data"]])
                    t.start()
    except Exception as e:
        print(f"ERRO NA PORTA UDP: {e}")
    print(f"PORTA UDP FECHADA")


def receive_file(filename, base64_data):
    try:
        data = base64.b64decode(base64_data)
        save_path = os.path.join(shared_folder, filename)

        with open(save_path, 'wb') as file:
            file.write(data)

        print(f"Ficheiro recebido e guardado em '{save_path}.")
        input("Pressionar Enter para voltar ao menu...")
        t = threading.Thread(target=update_tracker, args=[filename])
        t.start()
    except Exception as e:
        print(f"ERRO AO RECEBER FICHEIRO: {e}")


def update_tracker(filename):
    message = {
        "type": 2,
        "filename": filename
    }
    try:
        socket_tcp.sendto(json.dumps(message).encode(), tracker_host)
    except Exception as e:
        print(f"ERRO AO ATUALIZAR FICHEIRO NO TRACKER: {e}")


def send_file(filename, addr):
    file_path = os.path.join(shared_folder, filename)

    with open(file_path, 'rb') as file:
        file_data = file.read()

    message = {
        "type": FILE,
        "filename": filename,
        "base64_data": base64.b64encode(file_data).decode()
    }

    try:
        socket_udp.sendto(json.dumps(message).encode(), addr)
        # print(f"Ficheiro enviado")
    except Exception as e:
        print(f"ERRO AO ENVIAR FICHEIRO: {e}")


def connect_to_tracker():
    try:
        socket_tcp.connect((tracker_host, int(tracker_port)))
        file_names = []

        for _, _, files in os.walk(shared_folder):
            for file in files:
                file_names.append(file)
                files_available.append(file)

        socket_tcp.send(json.dumps(file_names).encode())
        print("Conexão FS Track Protocol com servidor " + tracker_host + " porta " + str(tracker_port))

    except Exception as e:
        print(f"Error connecting to the tracker: {e}")


def askForFile():
    filename = input("Que ficheiro quer?: ")
    if filename not in files_available:
        request = {
            "type": 1,
            "filename": filename
        }
        socket_tcp.send(json.dumps(request).encode())
        message = socket_tcp.recv(1024)
        nodes = json.loads(message.decode())
        if nodes:
            print("Clientes com o ficheiro pedido:")
            for node in nodes:
                print(f"{node}")
                input("Pressionar Enter para comecar transferencia...")
            message = {
                "type": FILE_REQUEST,
                "filename": filename
            }
            socket_udp.sendto(json.dumps(message).encode(), (nodes[0], 9090))
            # print(f"Ficheiro pedido a {nodes[0]}")
        else:
            print("Ficheiro nao encontrado")
            input("Pressionar Enter para voltar ao menu...")
    else:
        print("Ja possui o ficheiro pedido")
        input("Pressionar Enter para voltar ao menu...")


def disconnect():
    global exit_flag
    try:
        socket_tcp.send("-1".encode())
        socket_tcp.shutdown(socket.SHUT_RDWR)
        socket_tcp.close()
        socket_udp.close()
        exit_flag = True
    except Exception as e:
        print(f"Erro ao desconectar: {e}")


def clear_terminal():
    subprocess.call("clear", shell=True)


if __name__ == "__main__":
    main()
