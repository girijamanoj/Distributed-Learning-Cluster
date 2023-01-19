from mp2_constants import *
from datetime import datetime
import json
import socket
from message import message
import threading
from mp2_logger import get_logger
logger = get_logger("introducer")
membership_list = {}


class introducer:
    """First point of contact for the system"""

    def __init__(self, port):
        global logger
        self.message_obj = message()
        self.port = port
        self.logger = logger

    def serve(self):
        try:
            host = "fa22-cs425-1901"
            port = self.port
            print("Launching server with socket {}:{}".format(host, port))
            self.logger.debug("Launching server with socket {}:{}".format(host, port))
            self.server = socket.socket()
            self.server.bind((host, port))
            # It will enable the server object to listen to 10 concurrent queries.
            self.server.listen(10)

        except Exception as e:
            self.logger.debug(e)
            print("Server failed\n{}".format(str(e)))
        self.recv()

    def send_message(self, conn, message):
        """sends TCP messages"""
        try:
            conn.sendall(message)
        except Exception as e:
            self.logger.debug(e)

    def recv(self):
        """Listens for incoming connections and messages"""
        while True:
            self.conn, self.address = self.server.accept()
            self.address = self.address[0]
            while True:
                try:
                    self.conn
                except Exception as e:
                    print("Connection Error: {}".format(e))
                    self.logger.debug(e)
                    break
                try:
                    recvd_message = self.conn.recv(BUFFER_SIZE)
                    recvd_message = self.message_obj.decode_message(recvd_message)
                    if recvd_message["T"] == JOIN:
                        self.set_proc_id()
                        self.add_membership_list()
                        self.send_membership_list()
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
                except Exception as e:
                    print("Receive Error: {}".format(e))
                    self.logger.debug(e)
                    break

    def remove_membership_list(self, proc_id):
        """Updated the membership list"""
        global membership_list
        try:
            del membership_list[proc_id]
        except Exception as e:
            self.logger.debug(e)
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
        print(membership_list)

    def send_membership_list(self):
        """Send the existing membership list to the new process"""
        global membership_list
        payload = message(JOIN, list(membership_list.keys()))
        payload = payload.get_encoded_message()
        self.send_message(self.conn, payload)


def main():
    logger.debug("Starting servers")
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
