from tcp_connection import tcp_connection
from message import message
from mp2_constants import *
from mp3_constants import *
from utils import *
import os


class remover:
    def __init__(
        self,
        sdfs_file_name,
        membership_list=None,
        introducer_obj=None,
        is_server=False,
        self_proc_id=None,
    ):
        self.sdfs_file_name = sdfs_file_name
        self.introducer_obj = introducer_obj
        self.membership_list = membership_list
        self.self_proc_id = self_proc_id
        self.port = REMOVER_PORT
        self.message_decoder = message()

    def serve(self):
        """Starts the server"""
        try:
            host = HOSTNAME
            port = self.port
            self.server = socket.socket()
            self.server.bind((host, port))
            self.server.listen(10)
        except Exception as e:
            print("Server failed\n{}".format(str(e)))
        self.recv()

    def recv(self):
        """Listens for incoming connections and messages"""
        while True:
            try:
                conn, address = self.server.accept()
                address = address[0]
                recvd_message = conn.recv(FILE_NAME_SIZE)
                recvd_message = self.message_decoder.decode_message(recvd_message)
                file_name = recvd_message["D"].strip()
                conn.close()
                file_name, extension, version = get_file_name_ext_version(file_name)
                present = check_if_file_exists(file_name)
                if not present:
                    continue
                self.notify_nodes(file_name + extension)
                files = get_all_versions(file_name)
                self.delete_file(files)
            except Exception as r:
                print("Error in receiving file delete: {}".format(r))

    def delete_file(self, files):
        """Deletes the file from the local directory"""
        for file in files:
            os.remove("sdfs_files/" + file)

    def notify_nodes(self, file_name):
        """Notifies the nodes to remove the file from their local directory"""
        try:
            adjusted_file_name = adjust_file_name(file_name, DEL=True)
            payload = message(REMOVE, adjusted_file_name)
            payload = payload.get_encoded_message()
            for node in self.membership_list:
                try:
                    tcp_connection_obj = tcp_connection(
                        get_hostname_from_proc_id(node), REMOVER_PORT
                    )
                    tcp_connection_obj.connect_to_target()
                    tcp_connection_obj.conn.sendall(payload)
                    tcp_connection_obj.close_conn()
                except:
                    pass
            try:
                self.introducer_obj.notify_replication(payload)
            except:
                pass
            try:
                tcp_connection_obj = tcp_connection(HOSTNAME, REPLICATOR_PORT)
                tcp_connection_obj.connect_to_target()
                tcp_connection_obj.conn.sendall(payload)
                tcp_connection_obj.close_conn()
            except:
                pass
        except Exception as e:
            # print("Error in notifying nodes {}".format(e))
            pass

    def remove(self):
        """Informs the target nodes and introducer to remove the file"""
        try:
            payload = message(DELETE, adjust_file_name(self.sdfs_file_name, DEL=True))
            payload = payload.get_encoded_message()
            nodes = [get_hostname_from_proc_id(node) for node in self.membership_list]
            for node in nodes:
                try:
                    tcp_connection_obj = tcp_connection(node, REPLICATOR_PORT)
                    tcp_connection_obj.connect_to_target()
                    tcp_connection_obj.conn.sendall(payload)
                    tcp_connection_obj.close_conn()
                except Exception as e:
                    # print("Error in removing file from node {}: {}".format(node, e))
                    pass
            return 1
        except Exception as e:
            print("Error in notifying remove file for nodes {}".format(e))
            return 0
