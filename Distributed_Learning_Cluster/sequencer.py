from collections import defaultdict
from mp3_constants import *
from message import message
from mp2_constants import *
import threading


class sequencer:
    def __init__(self, metadata) -> None:
        self.metadata = metadata
        self.sequence = defaultdict(lambda: -1)
        self.message_decoder = message()

    def get_version_from_sdfs_file(self, filename):
        """Returns the version of the file"""
        temp = filename.split("_v")[1]
        temp = temp.split(".")[0]
        return int(temp)

    def get_file_name_from_sdfs_file(self, filename):
        """Returns the file name from the sdfs file name"""
        temp = filename.split("_v")[0]
        ext = filename.split(".")[-1]
        return temp + "." + ext

    def process_metadata(self):
        """Processes the metadata and returns the file name and version"""
        while True:
            for proc_id in list(self.metadata):
                for file_name in list(self.metadata[proc_id]):
                    normal_file_name = self.get_file_name_from_sdfs_file(file_name)
                    version = self.get_version_from_sdfs_file(file_name)
                    self.sequence[normal_file_name] = max(
                        self.sequence[normal_file_name], version
                    )

    def serve(self):
        try:
            host = HOSTNAME
            port = SEQUENCER_PORT
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
                self.sequence[file_name] += 1
                payload = message(SEQUENCER, str(self.sequence[file_name]))
                payload = payload.get_encoded_message()
                self.conn.sendall(payload)
                try:
                    self.conn.close()
                except Exception as e:
                    print("Error in closing seq connection: {}".format(e))
            except Exception as e:
                print("Error in sequencer", e)
