import socket
import sys
import threading

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
        (clientSocket, clientAddress) = tcpSocket.accept()
        print("Conectado a cliente: " + str(clientAddress))
        t = threading.Thread(target=connectionTask, args=[clientSocket, clientAddress])
        t.run()


# Função usada pelas threads das nodes
def connectionTask(clientSocket, clientAddress):
    fileName, address = clientSocket.recvfrom(1024)
    # Recebe o nome de todos os ficheiros
    while fileName != "b'-1'":
        if files.keys().__contains__(fileName):
            files[fileName].append(address)
        else:
            files[fileName] = []
        fileName, address = clientSocket.recvfrom(1024)
        print(fileName)

    # Verifica se a conexão é fechada ou recebe um nome de um ficheiro e envia todos os nodos associados a este
    # TODO: Adicionar timeouts
    while True:
        print("asdasdasdasd")
        message, address = clientSocket.recvfrom(1024)
        if message == "-1":
            print("asdasdasdasd")
            clientSocket.shutdown(socket.SHUT_RDWR)
            clientSocket.close()
            cleanClient(address)
        elif files[message] is not None:
            for address in files[message]:
                clientSocket.send(address)


def cleanClient(address):
    for file in files:
        for add in file:
            if add == address:
                files[file].remove(add)
    print("Cliente " + address + " desconectou")


if __name__ == '__main__':
    main()
