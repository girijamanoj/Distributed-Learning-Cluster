from tcp_connection import tcp_connection
from message import message
from mp2_constants import *
from mp3_constants import *
from utils import *
import os


class uploader:
    def __init__(
        self,
        local_file_name,
        sdfs_file_name,
        target,
        counter,
        membership_list=None,
        introducer_obj=None,
        is_server=False,
    ):
        self.target = target
        self.counter = counter
        self.local_file_name = local_file_name
        self.sdfs_file_name = sdfs_file_name
        self.introducer_obj = introducer_obj
        self.membership_list = membership_list
        self.port = 7000
        if is_server == False:
            self.conn_obj = tcp_connection(target, self.port)
            self.conn_obj.connect_to_target()
            self.conn = self.conn_obj.conn
        self.message_decoder = message()

    def serve(self):
        try:
            host = HOSTNAME
            port = self.port
            self.server = socket.socket()
            self.server.bind((host, port))
            # It will enable the server object to listen to 10 concurrent queries.
            self.server.listen(10)

        except Exception as e:
            # self.logger.debug(ip, str(e))
            print("Server failed\n{}".format(str(e)))
        self.recv()

    def recv(self):
        """Listens for incoming connections and messages"""
        while True:
            try:
                self.conn, self.address = self.server.accept()
                self.address = self.address[0]
                recvd_message = self.conn.recv(FILE_NAME_SIZE)
                recvd_message = self.message_decoder.decode_message(recvd_message)
                file_name = recvd_message["D"].strip()
                try:
                    with open("sdfs_files/" + file_name, "x") as f:
                        pass
                except Exception as e:
                    # print("Error in creating file: {}".format(e))
                    pass
                with open("sdfs_files/" + file_name, "wb") as file_data:
                    while True:
                        data = self.conn.recv(FILE_BUFFER_SIZE)
                        if not data:
                            break
                        file_data.write(data)
                try:
                    self.conn.close()
                except Exception as e:
                    print("conn error {}".format(e))
                self.notify_nodes(file_name)
            except Exception as r:
                print("Error in receiving file: {}".format(r))

    def notify_nodes(self, file_name):
        try:
            adjusted_file_name = adjust_file_name(file_name, True)
            payload = message(REPLICATOR, adjusted_file_name)
            payload = payload.get_encoded_message()
            for node in self.membership_list:
                tcp_connection_obj = tcp_connection(
                    get_hostname_from_proc_id(node), REPLICATOR_PORT
                )
                tcp_connection_obj.connect_to_target()
                tcp_connection_obj.conn.sendall(payload)
                tcp_connection_obj.close_conn()
            self.introducer_obj.notify_replication(payload)
            tcp_connection_obj = tcp_connection(HOSTNAME, REPLICATOR_PORT)
            tcp_connection_obj.connect_to_target()
            tcp_connection_obj.conn.sendall(payload)
            tcp_connection_obj.close_conn()
        except Exception as e:
            print("Error in notifying nodes {}".format(e))

    def get_sequencer_conn(self):
        nodes = [get_hostname_from_proc_id(node) for node in self.membership_list] + [
            HOSTNAME
        ]
        nodes.sort()
        sequencer = nodes[0]
        try:
            tcp_connection_obj = tcp_connection(sequencer, SEQUENCER_PORT)
            tcp_connection_obj.connect_to_target()
            return tcp_connection_obj
        except Exception as e:
            print("Error in getting sequencer connection: {}".format(e))

    def insert_version(self):
        temp = self.sdfs_file_name.split(".")
        self.sdfs_file_name = temp[0] + "_v" + str(self.seq_no) + "." + temp[1]

    def get_counter(self):
        seq_conn = self.get_sequencer_conn()
        payload = message(SEQUENCER, adjust_file_name(self.sdfs_file_name, True))
        payload = payload.get_encoded_message()
        seq_conn.conn.sendall(payload)
        recvd_message = seq_conn.conn.recv(FILE_NAME_SIZE)
        recvd_message = self.message_decoder.decode_message(recvd_message)
        self.seq_no = recvd_message["D"].strip()
        seq_conn.close_conn()

    def upload(self):
        try:
            self.sdfs_file_name = adjust_file_name(self.sdfs_file_name)
            payload = message(UPLOAD, self.sdfs_file_name)
            payload = payload.get_encoded_message()
            self.conn.send(payload)
            with open(self.local_file_name, "rb") as f:
                while True:
                    data = f.read(FILE_BUFFER_SIZE)
                    if not data:
                        break
                    self.conn.sendall(data)
            self.conn.close()
            self.counter.append(self.target)
        except Exception as e:
            print("Error in uploading file: {}".format(e))
