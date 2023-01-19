from constants import *
import socket
import threading
import json
from py_grepper import py_grepper
from functools import lru_cache
import logging


class Server:
    def __init__(self):

        self.port = PORT
        self.host = curr_host

        # setting the config of logging
        logging.basicConfig(
            filename="/var/log/server.log",
            filemode="wb",
            format="%(asctime)s %(name)s - %(message)s",
            datefmt="%d-%b-%y %H:%M:%S",
        )
        # logging at the start of the server
        self.logger = logging.getLogger()

    # Launches the server on the port.
    def serve(self):
        try:
            print("Launching server with socket {}:{}".format(self.host, self.port))
            self.server = socket.socket()
            self.server.bind((self.host, self.port))

            # It will enable the server object to listen to 10 concurrent queries.
            self.server.listen(10)

        except Exception as e:
            self.logger.debug(self.host, str(e))
            print("Server failed\n{}".format(str(e)))

    # Grep when caching is not used
    def grep_no_cache(self, query):
        """_summary_

        Args:
            query ([str]): contains the decoded query.

        Returns:
            count ([int]): count of matching lines.
        """
        py_grepper_obj = py_grepper()
        count = None
        args = json.loads(query)

        # checking if the query is pattern type.
        if args.get("pattern") != None and len(args.get("pattern")) != 0:
            count = py_grepper_obj.grep("Ec", args["pattern"])
        else:
            count = py_grepper_obj.grep("c", args["text"])
        return count

    # Grep when caching is used, Here we are using LRU(Least Recently Used) caching.
    @lru_cache(maxsize=None)
    def grep(self, query):
        """_summary_

        Args:
            query ([str]): contains the decoded query.

        Returns:
            count ([int]): count of matching lines.
        """
        py_grepper_obj = py_grepper()
        args = json.loads(query)
        count = None

        # checking if the query is pattern type.
        if args.get("pattern") != None and len(args.get("pattern")) != 0:
            count = py_grepper_obj.grep("Ec", args["pattern"])
        else:
            count = py_grepper_obj.grep("c", args["text"])
        return count

    # Server listens for the incoming query
    def listen(self):
        while 1:
            try:
                self.conn, self.address = self.server.accept()
                while 1:
                    try:
                        # recieveing the query
                        query = self.conn.recv(BUFFER_SIZE)
                    except Exception as e:
                        print(e)
                        self.logger.debug(self.host, str(e))
                        query = None
                    # stopping recieving as it recieved None (Either because of server failing or Query is completely recieved)
                    if query == b"" or len(query) == 0 or query is None:
                        break

                    query = query.decode("UTF-8")
                    temp = json.loads(query)

                    # Checking for the cache option
                    if temp.get("cached") == 0:
                        count = self.grep_no_cache(query)
                    else:
                        count = self.grep(query)
                    count = bytes(str(count), encoding="UTF-8")

                    # sending back the matching lines count to Client.
                    self.conn.sendall(count)

                # closing the connection.
                self.conn.close()
            except Exception as e:
                self.logger.debug(self.host, str(e))


def main():
    server = Server()

    # running the server.
    server.serve()

    # server starts listening for queries
    server.listen()


if __name__ == "__main__":
    main()
