import socket
import sys
import threading
import json

files = {}


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
        print("Conectado a cliente: " + str(clientIP) + " na porta " +str(clientPORT))
        t = threading.Thread(target=connectionTask, args=[clientSocket, clientIP])
        t.start()


# Função usada pelas threads das nodes
def connectionTask(clientSocket, clientIP):
    message = clientSocket.recv(1024).decode()
    file_names = json.loads(message)
    # Recebe o nome de todos os ficheiros
    stop = "-1"
    for file in file_names:
        if file in files:
            files[file].append(clientIP)
        else:
            files.update({file: [clientIP]})
    print(files)

    # Verifica se a conexão é fechada ou recebe um nome de um ficheiro e envia todos os nodos associados a este
    # TODO: Adicionar timeouts
    while True:
        message = clientSocket.recv(1024).decode()
        addressList = []
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
        elif message in files:
            addressList = files[message]
        addressListJson = json.dumps(addressList)
        clientSocket.send(addressListJson.encode())


def cleanClient(address):
    del_list = []
    for file, add in files.items():
        if address in add:
            add.remove(address)
        if len(add) == 0:
            del_list.append(file)
    for file in del_list:
        del files[file]


if __name__ == '__main__':
    main()
