import socket
import threading


def connectionTask(clientSocket, clientAddress):
    message = "a"
    while message != "-1":
        message = clientSocket.recv(1024)
        files[message] = []
        files[message].append(clientAddress)

    while True:
        message = clientSocket.recv(1024)
        if message == "-1":
            clientSocket.shutdown()
            clientSocket.close()
        elif files[message] != None:
            for address in files[message]:
                clientSocket.send(address)
                




files = {}

HOST = socket.gethostname()
PORT = 9090

# Criação do Socket
tcpSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
tcpSocket.bind((HOST, PORT))
tcpSocket.listen()
print("Servidor ativo em " + HOST + " porta " + str(PORT))

while True:
    (clientSocket, clientAddress) = tcpSocket.accept()
    t = threading.Thread(target=connectionTask, args=[clientSocket, clientAddress])
    t.run()
