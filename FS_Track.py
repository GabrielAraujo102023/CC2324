import socket
import sys
import threading

files = {}
HOST = "10.0.0.10"


def main():
    print("HOST IS " + HOST)
    PORT = 9090
    if len(sys.argv) == 2:  # Verifica se usa um Port costumizadoo
        PORT = int(sys.argv[1])
    # Criação do Socket
    tcpSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    tcpSocket.bind(("10.0.0.10", PORT))
    tcpSocket.listen()
    print("Servidor ativo em " + HOST + " porta " + str(PORT))

    # Fica à espera de conexões novas, cria uma thread para cada nodo que se conecta
    while True:
        (clientSocket, clientAddress) = tcpSocket.accept()
        print("conectado a cliente: " + str(clientAddress))
        t = threading.Thread(target=connectionTask, args=[clientSocket, clientAddress])
        t.run()


# Função usada pelas threads das nodes
def connectionTask(clientSocket, clientAddress):
    message = "a"
    # Recebe o nome de todos os ficheiros
    while message != "-1":
        data, addr = clientSocket.recv(1024)
        print(data.decode())
        files[data.decode()] = []
        files[data.decode()].append(clientAddress)

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
