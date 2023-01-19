from tcp_connection import tcp_connection
from message import message
from mp2_constants import *
from mp3_constants import *
from utils import *
from mp4_constants import *


class appender:
    """Appender class that runs on each node and appends the results to the file"""

    def __init__(
        self,
        sdfs_file_name,
        target,
        counter,
        membership_list=None,
        introducer_obj=None,
        is_server=False,
        results=None,
    ):
        self.target = target
        self.counter = counter
        self.sdfs_file_name = sdfs_file_name
        self.introducer_obj = introducer_obj
        self.membership_list = membership_list
        self.port = APPEND_PORT
        self.results = results
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
                try:
                    self.conn, self.address = self.server.accept()
                    self.address = self.address[0]
                    recvd_message = self.conn.recv(BUFFER_SIZE)
                    self.conn.close()
                except Exception as e:
                    print("conn error {}".format(e))
                    continue
                recvd_message = self.message_decoder.decode_message(recvd_message)
                if recvd_message["T"] == APPEND:
                    self.sdfs_file_name = str(recvd_message["D"]["job_id"]) + ".csv"
                    # self.get_counter()
                    self.insert_version()
                    file_name = self.sdfs_file_name
                    if not check_if_file_exists(file_name):
                        try:
                            with open("sdfs_files/" + file_name, "x") as f:
                                pass
                        except Exception as e:
                            pass
                    with open("sdfs_files/" + file_name, "a") as file_data:
                        for file in recvd_message["D"]["results"]:
                            file_data.write(
                                "\n" + file + "," + recvd_message["D"]["results"][file]
                            )
                self.notify_nodes(file_name)
            except Exception as r:
                print("Error in receiving file: {}".format(r))

    def notify_nodes(self, file_name):
        """notifies the nodes that the file has been appended"""
        try:
            adjusted_file_name = adjust_file_name(file_name, True)
            payload = message(REPLICATOR, adjusted_file_name)
            payload = payload.get_encoded_message()
            for node in self.membership_list:
                try:
                    tcp_connection_obj = tcp_connection(
                        get_hostname_from_proc_id(node), REPLICATOR_PORT
                    )
                    tcp_connection_obj.connect_to_target()
                    tcp_connection_obj.conn.sendall(payload)
                    tcp_connection_obj.close_conn()
                except Exception as e:
                    # print("Error in notifying nodes: {}".format(e))
                    pass
            try:
                self.introducer_obj.notify_replication(payload)
            except Exception as e:
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

    def append(self):
        """send the results to the appender"""
        try:
            payload = message(APPEND, self.results)
            payload = payload.get_encoded_message()
            self.conn.sendall(payload)
            self.conn.close()
            self.counter.append(self.target)
            return True
        except Exception as e:
            print("Error in appending file: {}".format(e))
            return False

    def insert_version(self):
        temp = self.sdfs_file_name.split(".")
        self.sdfs_file_name = temp[0] + "_v0." + temp[1]
