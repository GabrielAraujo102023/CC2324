from enum import Enum, auto


class MessageType(Enum):
    # Mensagens cliente -> server
    NEW_CONNECTION = auto()
    DISCONNECT = auto()
    OWNERS_REQUEST = auto()
    BLOCK_UPDATE = auto()
    FILE_INFO_REQUEST = auto()
    FILE_STATE_REQUEST = auto()

    # Mensagens server -> cliente
    OWNERS = auto()
    FILE_INFO = auto()
    FILE_STATE = auto()

    # Mensagens cliente -> cliente
    BLOCK_DATA = auto()
    BLOCK_DATA_ACK = auto()
    BLOCK_REQUEST = auto()
    BLOCK_REQUEST_ACK = auto()

    # Mensagens DNS
    DNS_REQUEST = auto()
    DNS_REPLY = auto()

# ESTRUTURAS DE MENSAGENS USADAS POR UM CLIENTE


# Ack de uma mensagem de pedido de blocos
class BlockRequestAckMessage:
    def __init__(self, file_name, corrupted):
        self.type = MessageType.BLOCK_REQUEST_ACK
        self.file_name = file_name
        self.corrupted = corrupted


# Ack de uma mensagem de dados de um bloco
class BlockDataAckMessage:
    def __init__(self, block_name, corrupted):
        self.type = MessageType.BLOCK_DATA_ACK
        self.block_name = block_name
        self.corrupted = corrupted


# Informar servidor sobre a pasta partilhada
class NewConnectionMessage:
    def __init__(self, files_info, blocks_info, node_name):
        self.type = MessageType.NEW_CONNECTION
        self.files_info = files_info
        self.blocks_info = blocks_info
        self.node_name = node_name


# Envio de dados de um bloco a um cliente
class BlockDataMessage:
    def __init__(self, block_name, block_data, block_hash):
        self.type = MessageType.BLOCK_DATA
        self.block_name = block_name
        self.block_data = block_data
        self.block_hash = block_hash


# Pedido de donos de um ficheiro ao servidor
class OwnersRequestMessage:
    def __init__(self, file_name):
        self.type = MessageType.OWNERS_REQUEST
        self.file_name = file_name


# Informar servidor que se vai desconetar
class DisconnectMessage:
    def __init__(self):
        self.type = MessageType.DISCONNECT


# Informar servidor de um novo bloco disponível
class BlockUpdateMessage:
    def __init__(self, block_name):
        self.type = MessageType.BLOCK_UPDATE
        self.block_name = block_name


# Pedido de informação sobre um ficheiro ao servidor
class FileInfoRequestMessage:
    def __init__(self, file_name):
        self.type = MessageType.FILE_INFO_REQUEST
        self.file_name = file_name


# Pedido de blocos a um cliente
class BlockRequestMessage:
    def __init__(self, file_name, blocks, data_hash):
        self.type = MessageType.BLOCK_REQUEST
        self.file_name = file_name
        self.blocks = blocks
        self.data_hash = data_hash


class FileStateRequestMessage:
    def __init__(self, file_name):
        self.type = MessageType.FILE_STATE_REQUEST
        self.file_name = file_name

# ESTRUTURAS DE MENSAGENS USADAS PELO SERVIDOR


# Informar cliente sobre donos de um ficheiro
class OwnersMessage:
    def __init__(self, owners, total_blocks):
        self.type = MessageType.OWNERS
        self.owners = owners
        self.total_blocks = total_blocks


# Informar cliente sobre um ficheiro
class FileInfoMessage:
    def __init__(self, file_hash, total_blocks):
        self.type = MessageType.FILE_INFO
        self.file_hash = file_hash
        self.total_blocks = total_blocks


class FileStateMessage:
    def __init__(self, available):
        self.type = MessageType.FILE_STATE
        self.available = available


class DnsRequest:
    def __init__(self, sender_name, requests, reply_token, delete=True):
        self.type = MessageType.DNS_REQUEST
        self.sender_name = sender_name
        self.requests = requests
        self.reply_token = reply_token
        self.delete = delete


class DnsReply:
    def __init__(self, reply_token, ips):
        self.type = MessageType.DNS_REPLY

        # reply_token é usado para identificar o uso da reply por quem a recebe. No nosso contexto, vai ser usado para
        # identificar se a mensagem recebida é relacionada a um pedido de transferência ('transf'), ou a um pedido de
        # blocos de outro nodo (nome desse nodo)
        self.reply_token = reply_token
        self.ips = ips
