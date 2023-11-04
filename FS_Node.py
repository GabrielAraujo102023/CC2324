import os
import subprocess
import sys
import socket
import threading
import json

if len(sys.argv) < 3 or len(sys.argv) > 4:
    print("Usage: FS_Node <share_folder> <address> <port>")
    sys.exit(1)

os.environ['TERM'] = 'xterm'
shared_folder = sys.argv[1]
tracker_host = sys.argv[2]
tracker_port = 9090
if len(sys.argv) == 4:
    tracker_port = sys.argv[3]
udp_port = 9090
block_size = 128
available_blocks = {}
node_info = {
    "address": "",
    "blocks": available_blocks,
}
socket_tcp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
socket_udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
exit_flag = False


def main():
    connect_to_tracker()
    socket_udp.bind(("0.0.0.0", udp_port))
    udp_thread = threading.Thread(target=file_transfer)
    udp_thread.start()

    while not exit_flag:
        clear_terminal()
        print("Conexão FS Track Protocol com servidor " + tracker_host + " porta " + str(tracker_port))
        print("FS Transfer Protocol: à escuta na porta UDP " + str(udp_port))
        print("1 - Transferir ficheiro\n"
              "2 - Fechar conexão\n"
              "Escolha:")
        choice = input()
        if choice == "1":
            askForFile()
        elif choice == "2":
            disconnect()


def file_transfer():
    try:
        while not exit_flag:
            data, addr = socket_udp.recvfrom(1024)
    except Exception as e:
        print(f"ERRO NA PORTA UDP: {e}")
    print("FECHEI A DA UDP")


def connect_to_tracker():
    try:
        socket_tcp.connect((tracker_host, int(tracker_port)))
        file_names = []

        for _, _, files in os.walk(shared_folder):
            for file in files:
                file_names.append(file)

        file_names_str = '\n'.join(file_names)
        socket_tcp.send(file_names_str.encode())

    except Exception as e:
        print(f"Error connecting to the tracker: {e}")


def askForFile():
    filename = input("Que ficheiro quer?: ")
    socket_tcp.send(filename.encode())
    message = socket_tcp.recv(1024)
    nodes = json.loads(message.decode())
    if nodes:
        print("Clientes com o ficheiro pedido:")
        for node in nodes:
            print(f"{node}")
            input("Pressionar Enter para comecar transferencia...")
    else:
        print("Ficheiro nao encontrado")
        input("Pressionar Enter para voltar ao menu...")


def disconnect():
    global exit_flag
    try:
        socket_tcp.send("-1".encode())
        socket_tcp.shutdown(socket.SHUT_RDWR)
        socket_tcp.close()
        socket_udp.close()
        print("DESCONETADO COM SUCESSO")
        exit_flag = True
        # sys.exit()
        # O PROGRAMA NAO TERMINA. A THREAD DA UDP CRIADA NA MAIN NAO FECHA. FICA PRESA NO WHILE DO FILE_TRANSFER
        # SE METER PARA LA UM socket.timeout() ACABA POR FECHAR, MAS É UMA CAGADA E DÁ OUTRO ERRO NA MESMA
        # ACHO QUE A SOCKET FECHA AQUI NO socket_udp.close().
    except Exception as e:
        print(f"Erro ao desconectar: {e}")


def clear_terminal():
    subprocess.call("clear", shell=True)


if __name__ == "__main__":
    main()
