import socket


class tcp_connection:
    def __init__(self, target, port) -> None:
        self.target = target
        self.port = port

    def connect_to_target(self):
        """Opens socket and connects to node"""
        try:
            temp_conn = socket.socket()
            temp_conn.connect((self.target, self.port))
            self.conn = temp_conn
        except Exception as e:
            print("{}:{} is down!, {}".format(self.target, self.port, str(e)))

    def close_conn(self):
        """closes socket"""
        try:
            self.conn.close()
        except:
            pass
