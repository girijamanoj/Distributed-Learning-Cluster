from tcp_connection import tcp_connection
from message import message
from mp3_constants import *
from mp2_constants import *
from utils import *


class downloader:
    """Generic function to download and replicate files"""

    def __init__(
        self,
        sdfs_file_name,
        local_file_name,
        node,
        acker_obj,
        is_replicator=False,
        is_server=False,
    ):
        self.sdfs_file_name = sdfs_file_name
        self.local_file_name = local_file_name
        self.node = node
        self.port = 9000
        self.acker_obj = acker_obj
        self.is_replicator = is_replicator
        if is_server == False:
            self.conn_obj = tcp_connection(node, self.port)
            self.conn_obj.connect_to_target()
            self.conn = self.conn_obj.conn
        self.message_decoder = message()

    def send_acks(self):
        """Respond with ACK

        Args:
            address (str): IP of the host to which ACK should be sent
        """
        if len(self.acker_obj.queue) > 2:
            for i in range(10):
                ack_no = None
                self.acker_obj.lock.acquire()
                try:
                    # print("queue during download: ",self.acker_obj.queue)
                    if len(self.acker_obj.queue) != 0:
                        address, ack_no = self.acker_obj.queue.popleft()
                except Exception as e:
                    print("Exception in download ack: ", e)
                finally:
                    self.acker_obj.lock.release()
                if ack_no is None:
                    return
                payload = message(ACK, ack_no)
                payload = payload.get_encoded_message()
                self.acker_obj.sock.sendto(payload, (address, self.port))
                self.acker_obj.tracker_obj.update_ack(address, ack_no)

    def download(self):
        """Downloads the file from the node"""
        payload = message(DOWNLOAD, adjust_file_name(self.sdfs_file_name))
        payload = payload.get_encoded_message()
        self.conn.sendall(payload)
        base = "downloads/"
        if self.is_replicator:
            base = "sdfs_files/"
        try:
            with open(base + self.local_file_name, "x") as f:
                pass
        except Exception as e:
            pass
        try:
            with open(base + self.local_file_name, "wb") as file_data:
                while True:
                    data = self.conn.recv(BUFFER_SIZE)
                    if not data:
                        break
                    file_data.write(data)
            self.conn_obj.close_conn()
        except Exception as e:
            print("Error in downloading file: {}".format(e))

    def serve(self):
        """Starts the download server"""
        try:
            host = HOSTNAME
            port = self.port
            self.server = socket.socket()
            self.server.bind((host, port))
            # It will enable the server object to listen to 10 concurrent queries.
            self.server.listen(10)

        except Exception as e:
            print("Server failed\n{}".format(str(e)))
        self.recv()

    def recv(self):
        """Listens for incoming connections and download requests"""
        while True:
            try:
                self.conn, self.address = self.server.accept()
                self.address = self.address[0]
                recvd_message = self.conn.recv(FILE_NAME_SIZE)
                recvd_message = self.message_decoder.decode_message(recvd_message)
                file_name = recvd_message["D"].strip()
                ind = file_name.rfind(".")
                file_name = file_name[:ind]
                file_name = get_latest_version(file_name)
                with open("sdfs_files/" + file_name, "rb") as f:
                    while True:
                        data = f.read(BUFFER_SIZE)
                        if not data:
                            break
                        self.send_acks()
                        self.conn.sendall(data)
                try:
                    self.conn.close()
                except Exception as e:
                    print("downloader line 84", e)
                    pass
            except Exception as e:
                print(e)
                pass
