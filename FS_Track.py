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
        clientSocket, clientAddress = tcpSocket.accept()
        print("Conectado a cliente: " + str(clientAddress))
        t = threading.Thread(target=connectionTask, args=[clientSocket, clientAddress[0]])
        t.start()


# Função usada pelas threads das nodes
def connectionTask(clientSocket, clientAddress):
    message = clientSocket.recv(1024).decode()
    fileNames = message.split('\n')
    # Recebe o nome de todos os ficheiros
    stop = "-1"
    for file in fileNames:
        if file != stop:
            if file in files:
                files[file].append(clientAddress)
            else:
                files.update({file: [clientAddress]})
            print(file)

    # Verifica se a conexão é fechada ou recebe um nome de um ficheiro e envia todos os nodos associados a este
    # TODO: Adicionar timeouts
    while True:
        print(files)
        print("ESPERANDO")
        message = clientSocket.recv(1024).decode()
        addressList = []
        print("message -> " + str(message))
        if message == stop:
            try:
                clientSocket.shutdown(socket.SHUT_RDWR)
            except Exception as e:
                print(f"Erro a desativar a socket: {e}")
            clientSocket.close()
            print("Cliente " + str(clientAddress) + " desconectou")
            cleanClient(clientAddress)
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
