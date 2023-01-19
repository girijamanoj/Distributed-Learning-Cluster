from mp2_constants import *
from datetime import datetime
from mp3_constants import *
import socket
from message import message
from collections import defaultdict
import threading
from utils import *

membership_list = {}
metadata = defaultdict(set)


class introducer:
    """First point of contact for the system"""

    def __init__(self, port):
        self.message_obj = message()
        self.port = port
        global metadata
        self.metadata = metadata

    def serve(self):
        try:
            host = "introducer"
            port = self.port
            print("Launching server with socket {}:{}".format(host, port))
            self.server = socket.socket()
            self.server.bind((host, port))
            # It will enable the server object to listen to 10 concurrent queries.
            self.server.listen(10)

        except Exception as e:
            # self.logger.debug(ip, str(e))
            print("Server failed\n{}".format(str(e)))
        self.recv()

    def send_message(self, conn, message):
        """sends TCP messages"""
        try:
            conn.sendall(message)
        except Exception as e:
            print(" introducer line 42 ", e)
            pass

    def recv(self):
        """Listens for incoming connections and messages"""
        while True:
            global membership_list
            self.conn, self.address = self.server.accept()
            self.address = self.address[0]
            while True:
                try:
                    self.conn
                except Exception as e:
                    print("Connection Error: {}".format(e))
                    break
                try:
                    recvd_message = self.conn.recv(BUFFER_SIZE)
                    recvd_message = self.message_obj.decode_message(recvd_message)
                    if recvd_message["T"] == JOIN:
                        self.set_proc_id()
                        self.add_membership_list()
                        self.send_membership_list_metadata()
                        self.notify_join()
                    elif recvd_message["T"] == LEAVE or recvd_message["T"] == FAILURE:
                        proc_id = recvd_message["D"]
                        print(
                            IP_HOSTNAME_MAP[self.address]
                            + " SAID that "
                            + proc_id
                            + " is dead"
                        )
                        self.remove_membership_list(proc_id)
                    elif recvd_message["T"] == REPLICATOR:
                        host_name = IP_HOSTNAME_MAP[self.address]
                        proc_id = get_proc_id_from_hostname(host_name, membership_list)
                        if proc_id == "" or proc_id is None:
                            continue
                        self.update_metadata(proc_id, recvd_message["D"])
                    elif recvd_message["T"] == REMOVE:
                        host_name = IP_HOSTNAME_MAP[self.address]
                        proc_id = get_proc_id_from_hostname(host_name, membership_list)
                        self.remove_metadata(proc_id, recvd_message["D"])
                    elif recvd_message["T"] == DELETE:
                        self.remove_metadata(proc_id, recvd_message["D"])
                except Exception as e:
                    print("Receive Error: {}".format(e))
                    break

    def remove_metadata(self, proc_id, file_name):
        """Removes the file from the metadata"""
        normal_file_name, _, _ = get_file_name_ext_version(file_name)
        for meta_file_name in list(self.metadata[proc_id]):
            if meta_file_name.startswith(normal_file_name):
                self.metadata[proc_id].remove(meta_file_name)

    def update_metadata(self, proc_id, metadata):
        """Updates the metadata"""
        file_name = metadata.strip()
        self.metadata[proc_id].add(file_name)
        print(self.metadata)

    def remove_membership_list(self, proc_id):
        """Updated the membership list"""
        global membership_list
        try:
            del membership_list[proc_id]
            del self.metadata[proc_id]
        except Exception as e:
            print(e)

    def notify_join(self):
        """Informs existing nodes abot the new process"""
        global membership_list
        payload = message(JOIN, [self.proc_id])
        payload = payload.get_encoded_message()
        for member in membership_list:
            if member != self.proc_id:
                self.send_message(membership_list[member], payload)

    def set_proc_id(self):
        """Assigns Process ID to the new process"""
        now = datetime.now().strftime("%H:%M:%S")
        proc_id = IP_HOSTNAME_MAP[self.address] + ":" + now
        self.proc_id = proc_id

    def add_membership_list(self):
        """Updated local membership list"""
        global membership_list
        membership_list[self.proc_id] = self.conn
        self.metadata[self.proc_id] = set()
        print(membership_list)

    def send_membership_list_metadata(self):
        """Send the existing membership list to the new process"""
        global membership_list
        temp_metadata = {}
        for proc_id in self.metadata:
            temp_metadata[proc_id] = list(self.metadata[proc_id])
        data = {
            "membership_list": list(membership_list.keys()),
            "metadata": temp_metadata,
        }
        payload = message(JOIN, data)
        payload = payload.get_encoded_message()
        print(payload)
        self.send_message(self.conn, payload)


def main():
    threads = []
    for i in range(10):
        introducer_obj = introducer(6001 + i)
        threads.append(threading.Thread(target=introducer_obj.serve))

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()


if __name__ == "__main__":
    main()
