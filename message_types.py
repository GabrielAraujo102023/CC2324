from enum import Enum, auto


class MessageType(Enum):
    # Mensagens cliente -> server
    NEW_CONNECTION = auto()
    DISCONNECT = auto()
    OWNERS_REQUEST = auto()
    BLOCK_UPDATE = auto()
    FILE_INFO_REQUEST = auto()

    # Mensagens server -> cliente
    OWNERS = auto()
    FILE_INFO = auto()

    # Mensagens cliente -> cliente
    BLOCK_DATA = auto()
    BLOCK_DATA_ACK = auto()
    BLOCK_REQUEST = auto()
    BLOCK_REQUEST_ACK = auto()


class BlockRequestAckMessage:
    def __init__(self, file_name, corrupted):
        self.type = MessageType.BLOCK_REQUEST_ACK
        self.file_name = file_name
        self.corrupted = corrupted


class BlockDataAckMessage:
    def __init__(self, block_name, corrupted):
        self.type = MessageType.BLOCK_DATA_ACK
        self.block_name = block_name
        self.corrupted = corrupted


class NewConnectionMessage:
    def __init__(self, files_info, blocks_info):
        self.type = MessageType.NEW_CONNECTION
        self.files_info = files_info
        self.blocks_info = blocks_info


class BlockDataMessage:
    def __init__(self, block_name, block_data, block_hash):
        self.type = MessageType.BLOCK_DATA
        self.block_name = block_name
        self.block_data = block_data
        self.block_hash = block_hash


class OwnersRequestMessage:
    def __init__(self, file_name):
        self.type = MessageType.OWNERS_REQUEST
        self.file_name = file_name


class OwnersMessage:
    def __init__(self, owners):
        self.type = MessageType.OWNERS
        self.owners = owners


class DisconnectMessage:
    def __init__(self):
        self.type = MessageType.DISCONNECT


class BlockUpdateMessage:
    def __init__(self, block_name):
        self.type = MessageType.BLOCK_UPDATE
        self.block_name = block_name


class FileInfoMessage:
    def __init__(self, file_hash, total_blocks):
        self.type = MessageType.FILE_INFO
        self.file_hash = file_hash
        self.total_blocks = total_blocks


class FileInfoRequestMessage:
    def __init__(self, file_name):
        self.type = MessageType.FILE_INFO_REQUEST
        self.file_name = file_name


class BlockRequestMessage:
    def __init__(self, file_name, blocks, data_hash):
        self.type = MessageType.BLOCK_REQUEST
        self.file_name = file_name
        self.blocks = blocks
        self.data_hash = data_hash
