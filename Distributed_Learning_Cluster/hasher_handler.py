import hashlib
from tcp_connection import tcp_connection
from mp2_constants import *
from mp3_constants import *
from message import message
from collections import defaultdict
from utils import *


class hash_handler:
    def __init__(self) -> None:
        self.host = HOSTNAME
        self.port = 8000
        self.message_decoder = message()

    def serve(self):
        """Starts the server"""
        try:
            self.server = socket.socket()
            self.server.bind((self.host, self.port))
            # It will enable the server object to listen to 10 concurrent queries.
            self.server.listen(10)

        except Exception as e:
            print("Server failed\n{}".format(str(e)))
        self.recv()

    def recv(self):
        """Listens for incoming connections and messages"""
        while True:
            self.conn, self.address = self.server.accept()
            self.address = self.address[0]
            try:
                self.conn
            except Exception as e:
                break
            try:
                recvd_message = self.conn.recv(FILE_NAME_SIZE)
                recvd_message = self.message_decoder.decode_message(recvd_message)
                file_name = recvd_message["D"].strip()
                hash = self.get_local_hash(file_name)
                payload = message(HASH_ACK, hash)
                payload = payload.get_encoded_message()
                self.conn.sendall(payload)
            finally:
                self.conn.close()

    def get_local_hash(self, file_name, is_downloader=False, is_replicator=False):
        """Returns the hash of the local file"""
        sha1 = hashlib.sha1()
        file_name, extension, version = get_file_name_ext_version(file_name)
        file_name = get_latest_version(file_name, is_downloader, is_replicator)
        base = "sdfs_files/"
        if is_downloader:
            base = "downloads/"
        if is_replicator:
            base = "sdfs_files/"
        with open(base + file_name, "rb") as file_data:
            while True:
                data = file_data.read(BUFFER_SIZE)
                if not data:
                    break
                sha1.update(data)
        return sha1.hexdigest()

    def get_remote_hash(self, file_name, nodes):
        """Returns the hash of the remote file"""
        node_hash = {}
        payload = message(HASH, adjust_file_name(file_name))
        payload = payload.get_encoded_message()
        c = defaultdict(list)
        for node in nodes:
            try:
                conn_obj = tcp_connection(get_hostname_from_proc_id(node), 8000)
            except Exception as e:
                print("Exception in get_remote_hash: {}".format(str(e)))
            conn_obj.connect_to_target()
            try:
                conn_obj.conn.sendall(payload)
                # print(payload)
            except Exception as e:
                print("Exception in get_remote_hash line 73: {}".format(str(e)))
                continue
            # print("Sent hash request to {}".format(node))
            data = conn_obj.conn.recv(FILE_NAME_SIZE)
            data = self.message_decoder.decode_message(data)
            c[data["D"]].append(get_hostname_from_proc_id(node))
        majority_hash = None
        majority_hash_count = 0
        for hash in c:
            if len(c[hash]) > majority_hash_count:
                majority_hash_count = len(c[hash])
                majority_hash = hash
        majority_nodes = c[majority_hash]
        return majority_hash, majority_nodes
