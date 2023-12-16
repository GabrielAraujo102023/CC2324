import os
import socket
import subprocess
import threading
import time
import message_types
import pickle

# Tamanho do buffer
BUFFER_SIZE = 1024

# Dicionário que liga nomes ao seu IP
names = {}

# Tempo de vida de uma entrada nos dicionários de nomes, é decrementado conforme o UPDATE_TIMER e quando chega a 0
# o nome é apagado da memória
TIME_TO_LIVE = 20

# Tempo, em segundos, até ser atualizada a lista de nomes, que vai apagar um nome ou decrementar o tempo de vida
UPDATE_TIMER = 60

# Socket que o DNS usa, em UDP
dns_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

# Lock para utilizar o dicionário de nomes
lock = threading.Lock()

# IP do DNS, estático, tem de ser mudado conforme a topologia do core
DNS_IP = "10.0.7.10"

# Port do DNS
DNS_PORT = 9091

os.environ['TERM'] = 'xterm'


def clear_terminal():
    subprocess.call("clear", shell=True)


def main():
    clear_terminal()
    dns_socket.bind(("", DNS_PORT))
    print("DNS ligado")
    threading.Thread(target=update_task).start()
    while True:
        pickle_msg, (ip, port) = dns_socket.recvfrom(BUFFER_SIZE)
        threading.Thread(target=reply_task, args=[pickle_msg, ip, port]).start()


# Esta é a função que trata de receber pedidos e guardar nomes e IPs em memória
def reply_task(pickle_msg, ip, port):
    try:
        message = pickle.loads(pickle_msg)
    except pickle.UnpicklingError as e:
        print(f"Erro a converter mensagem recebida: {e}")
    else:
        if message.type == message_types.MessageType.DNS_REQUEST:
            with lock:
                cur_ttl = TIME_TO_LIVE
                if not message.delete:
                    cur_ttl = -1
                # Guarda o nome e IP de quem enviou o pedido
                names[message.sender_name] = (ip, cur_ttl)
                print("Received request from " + message.sender_name)
                # Vê se existe pedidos na mensagem. Apenas o tracker é que envia uma mensagem sem pedidos quando é
                # ligado para ser guardado o seu IP
                if len(message.requests) > 0:
                    replies = []
                    for request in message.requests:
                        print("he wants " + request)
                        # Ignora pedidos que não estão guardados
                        if request not in names:
                            print(request + " não está registado.")
                            continue
                        (req, req_timer) = names[request]
                        if req_timer != -1:
                            names[request] = (req, TIME_TO_LIVE)
                        replies.append(req)
                    # Responde com o token do pedido e as respostas
                    reply = message_types.DnsReply(message.reply_token, replies)
                    dns_socket.sendto(pickle.dumps(reply), (ip, port))
                    print("mandou")


# Esta função trata de decrementar o TIME_TO_LIVE de cada entrada, a cada UPDATE_TIMER segundos, e quando vê que uma
# das entradas está a 0, apaga-a
def update_task():
    while True:
        # Espera um certo tempo
        time.sleep(UPDATE_TIMER)
        # Guarda todos os nomes que têm de ser removidos
        to_remove = []
        with lock:
            dict_iter = iter(names.items())
            for _ in names:
                name, (ip, timer) = next(dict_iter)
                if timer == 0:
                    to_remove.append(name)
                elif timer != -1:
                    names[name] = (ip, timer - 1)
            # Remove todos os nomes que ultrapassaram o tempo limite de vida sem ser pedidos
            for name in to_remove:
                names.pop(name)
                print("removi " + name)


if __name__ == "__main__":
    main()


# Esta função está aqui para ser usada pelo tracker e node, e ser desnecessário programá-la igualmente em dois sítios
# diferentes, ou ter um ficheiro só para esta função.
# Envia pedidos ao DNS

