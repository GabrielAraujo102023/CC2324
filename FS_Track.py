import socket
import sys
import threading

files = {}
HOST = socket.gethostname()


def main():
    # Verifica se usa um Port costumizadoo
    PORT = 9090
    if len(sys.argv) == 2:
        PORT = sys.argv[1]
    # Criação do Socket
    tcpSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    tcpSocket.bind((HOST, PORT))
    tcpSocket.listen()
    print("Servidor ativo em " + HOST + " porta " + str(PORT))

    # Fica à espera de conexões novas, cria uma thread para cada nodo que se conecta
    while True:
        (clientSocket, clientAddress) = tcpSocket.accept()
        t = threading.Thread(target=connectionTask, args=[clientSocket, clientAddress])
        t.run()


# Função usada pelas threads das nodes
def connectionTask(clientSocket, clientAddress):
    message = "a"
    # Recebe o nome de todos os ficheiros
    while message != "-1":
        message = clientSocket.recv(1024)
        files[message] = []
        files[message].append(clientAddress)

    # Verifica se a conexão é fechada ou recebe um nome de um ficheiro e envia todos os nodos associados a este
    # TODO: Adicionar timeouts
    while True:
        message = clientSocket.recv(1024)
        if message == "-1":
            clientSocket.shutdown()
            clientSocket.close()
        elif files[message] is not None:
            for address in files[message]:
                clientSocket.send(address)
        clientSocket.send("-1")


if __name__ == '__main__':
    main()
