import socket
import threading
import time

import message_types
import pickle

BUFFER_SIZE = 1024
names = {}
TIME_TO_LIVE = 1
UPDATE_TIMER = 20
dns_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
lock = threading.Lock()
DNS_IP = "10.0.7.10"
DNS_PORT = 9091


def main():
    dns_socket.bind(("", DNS_PORT))
    print("DNS ligado")
    threading.Thread(target=update_task).start()
    while True:
        pickle_msg, (ip, port) = dns_socket.recvfrom(BUFFER_SIZE)
        threading.Thread(target=reply_task, args=[pickle_msg, ip, port]).start()


def reply_task(pickle_msg, ip, port):
    try:
        message = pickle.loads(pickle_msg)
    except pickle.UnpicklingError as e:
        print(f"Erro a converter mensagem recebida: {e}")
    else:
        if message.type == message_types.MessageType.DNS_REQUEST:
            with lock:
                names[message.sender_name] = (ip, TIME_TO_LIVE)
                print("Received request from " + message.sender_name)
                if len(message.requests) > 0:
                    replies = []
                    for request in message.requests:
                        print("he wants " + request)
                        if request not in names:
                            print(request + " não está registado.")
                            continue
                        (req, _) = names[request]
                        names[request] = (req, TIME_TO_LIVE)
                        replies.append(req)
                    reply = message_types.DnsReply(message.reply_token, replies)
                    print((ip, port))
                    dns_socket.sendto(pickle.dumps(reply), (ip, port))
                    print("mandou")


def update_task():
    while True:
        time.sleep(UPDATE_TIMER)
        to_remove = []
        with lock:
            dict_iter = iter(names.items())
            for _ in names:
                name, (ip, timer) = next(dict_iter)
                if timer == 0:
                    to_remove.append(name)
                else:
                    names[name] = (ip, timer - 1)
            for name in to_remove:
                names.pop(name)
                print("removi " + name)


if __name__ == "__main__":
    main()


# Esta função está aqui para ser usada pelo tracker e node, e ser desnecessário programá-la igualmente em dois sítios
# diferentes, ou ter um ficheiro só para esta função.
def contact_dns(sender_name, udp_socket, requested_names, reply_token):
    dns_message = message_types.DnsRequest(sender_name, requested_names, reply_token)
    udp_socket.sendto(pickle.dumps(dns_message), (DNS_IP, DNS_PORT))
