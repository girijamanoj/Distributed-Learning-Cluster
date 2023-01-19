import os
import socket

BUFFER_SIZE = 1024
PING = 0
ACK = 1
FAILURE = 2
LEAVE = 3
JOIN = 4


HOSTNAME = os.popen("hostname").read().split(".")[0]
TIMEOUT = 2
PING_TIME = 1

HOSTNAME_IP_MAP = {}
IP_HOSTNAME_MAP = {}
for i in range(1, 11):
    temp = "fa22-cs425-19" + str(i).zfill(2)
    temp_ip = socket.gethostbyname(temp)
    HOSTNAME_IP_MAP[temp] = temp_ip
    IP_HOSTNAME_MAP[temp_ip] = temp
