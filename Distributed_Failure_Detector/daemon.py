import json
import socket
from mp2_constants import *
from message import message
from utils import *
from datetime import datetime
import threading
from time import sleep
from collections import defaultdict
from mp2_logger import get_logger
from prettytable import PrettyTable


INTRODUCER_CONN = [None]
SELF_PROC_ID = ""
logger = get_logger("daemon")


class tracker:
    """This calss is responsible for tacking pings and concurrency control"""

    def __init__(self):
        global logger
        self.logger = logger
        self.ping_dict = defaultdict(int)
        self.membership_list = []
        self.monitor_list = []
        self.monitor_lock = threading.Lock()
        self.ping_lock = threading.Lock()
        self.membership_list_lock = threading.Lock()
        payload = message(PING, "")
        self.introducer_obj = introducer()
        self.payload = payload.get_encoded_message()
        self.sender_obj = sender(self)

    def track(self):
        while True:
            try:
                self.monitor_lock.acquire()
                self.ping_lock.acquire()
                self.membership_list_lock.acquire()
                for member in self.monitor_list:
                    if self.ping_dict[member] <= 2:
                        continue
                    proc_id = get_proc_id_from_hostname(member, self.membership_list)
                    self.remove_member_in_membership_list(proc_id, locked=True)
                    self.update_monitor(locked=True)
                    self.introducer_obj.send_fail(proc_id)
                    self.sender_obj.send_fail(proc_id)
            except Exception as e:
                print(e)
                self.logger.error(e)
                
            finally:
                self.monitor_lock.release()
                self.ping_lock.release()
                self.membership_list_lock.release()
            sleep(0.5)

    def inc_ping(self, member):
        """This functions will increase the ping count

        Args:
            member (str): The hostname to which the ping is sent
        """
        self.ping_lock.acquire()
        try:
            self.ping_dict[member] += 1
        finally:
            self.ping_lock.release()

    def dec_ping(self, member):
        """This functions will decrease the ping count

        Args:
            member (str): The hostname to which the ping is sent
        """
        if member not in self.ping_dict:
            return
        self.ping_lock.acquire()
        try:
            self.ping_dict[member] -= 1
        finally:
            self.ping_lock.release()

    def add_member(self, proc_id):
        """This function will add new members to the membership list

        Args:
            proc_id (str): The process id of the new member
        """
        self.membership_list_lock.acquire()
        try:
            self.membership_list.append(proc_id)
        finally:
            self.membership_list_lock.release()

    def remove_member_in_membership_list(self, proc_id, locked=False):
        """This function will remove the members who are failed/left from the system

        Args:
            proc_id (str): The process id of the failed/left member
            locked (bool, optional): This will avoid deadlock while removing members. Defaults to False.
        """
        if locked == False:
            self.membership_list_lock.acquire()
        try:
            self.membership_list.remove(proc_id)
        finally:
            if locked == False:
                self.membership_list_lock.release()

    def update_monitor(self, locked=False):
        """This function will rebalance the system

        Args:
            locked (bool, optional): This will avoid deadlock while rebalancing the system. Defaults to False.. Defaults to False.
        """
        if locked == False:
            self.ping_lock.acquire()
            self.membership_list_lock.acquire()
            self.monitor_lock.acquire()
        try:
            host_num = get_host_number_from_hostname(HOSTNAME)
            temp_list = []
            for i in range(host_num, host_num + 9):
                x = (i % 10) + 1
                possible_mon = get_hostname_from_host_num(x)
                for item in self.membership_list:
                    if possible_mon in item:
                        temp_list.append(possible_mon)
                if len(temp_list) == 3:
                    break
            for member in self.monitor_list:
                if member not in temp_list:
                    self.monitor_list.remove(member)
            for member in temp_list:
                if member not in self.monitor_list:
                    self.monitor_list.append(member)
            temp_mon_list = self.monitor_list[:]
            for member in temp_mon_list:
                if member not in self.ping_dict:
                    self.ping_dict[member] = 0
            temp = list(self.ping_dict.keys())
            for member in temp:
                if member not in temp_mon_list:
                    del self.ping_dict[member]
            for member in self.ping_dict:
                self.ping_dict[member] = 0
        finally:
            if locked == False:
                self.ping_lock.release()
                self.membership_list_lock.release()
                self.monitor_lock.release()


class listner:
    def __init__(self, tracker_obj):
        """This will listen incoming messages from it's peer

        Args:
            tracker_obj (tracker): This will enable the listner class to access tracker for shared data
        """
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(("0.0.0.0", 6000))
        self.sender_obj = sender(tracker_obj)
        self.introducer_obj = introducer(tracker_obj=tracker_obj)
        self.tracker_obj = tracker_obj

    def recv(self):
        """This function will receive messages from its peers and triggers respective functions."""
        while True:
            recvd_message, addr = self.sock.recvfrom(BUFFER_SIZE)
            addr = addr[0]
            addr = IP_HOSTNAME_MAP[addr]
            message_obj = message()
            recvd_message = message_obj.decode_message(recvd_message)
            if recvd_message["T"] == PING:
                self.sender_obj.send_ack(addr)
            elif recvd_message["T"] == ACK:
                self.tracker_obj.dec_ping(addr)
            elif recvd_message["T"] == FAILURE:
                proc_id = recvd_message["D"]
                if proc_id not in self.tracker_obj.membership_list:
                    pass
                else:
                    self.introducer_obj.send_fail(proc_id)
                    self.tracker_obj.remove_member_in_membership_list(proc_id)
                    self.tracker_obj.update_monitor()
                    self.sender_obj.send_fail(proc_id)
            elif recvd_message["T"] == LEAVE:
                proc_id = recvd_message["D"]
                if proc_id not in self.tracker_obj.membership_list:
                    pass
                else:
                    self.introducer_obj.send_leave(proc_id)
                    self.tracker_obj.remove_member_in_membership_list(proc_id)
                    self.tracker_obj.update_monitor()
                    self.sender_obj.send_leave(proc_id)


class sender:
    def __init__(self, tracker_obj=None):
        """This class is responsible to send messages to it's peers

        Args:
            tracker_obj (tracker, optional): This will enable sender class to access shared data in tracker. Defaults to None.
        """
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.port = 6000
        self.tracker_obj = tracker_obj
        payload = message(PING, "")
        self.payload = payload.get_encoded_message()

    def ping(self):
        """Ping the members whom this process it monitoring"""
        for member in self.tracker_obj.monitor_list:
            self.sock.sendto(self.payload, (member, self.port))
            self.tracker_obj.inc_ping(member)

    def send_ack(self, address):
        """Respond with ACK

        Args:
            address (str): IP of the host to which ACK should be sent
        """
        payload = message(ACK, "")
        payload = payload.get_encoded_message()
        self.sock.sendto(payload, (address, self.port))

    def send_leave(self, proc_id):
        """This will notify it's peers about the members that are left or it's leave

        Args:
            proc_id (str): Process ID of the process leaving/left
        """
        monitored_by = []
        host_num = get_host_number_from_hostname(HOSTNAME)
        for i in range(host_num - 2, host_num - 11, -1):
            x = (i % 10) + 1
            possible_mon = get_hostname_from_host_num(x)
            for member in self.tracker_obj.membership_list:
                member = get_hostname_from_proc_id(member)
                if possible_mon in member:
                    monitored_by.append(member)
                    break
            if len(monitored_by) == 3:
                break
        for member in monitored_by:
            member = get_hostname_from_proc_id(member)
            payload = message(LEAVE, proc_id)
            payload = payload.get_encoded_message()
            self.sock.sendto(payload, (member, self.port))

    def send_fail(self, proc_id):
        """This will notify it's peer about the failes processes

        Args:
            proc_id (str): Process ID of the process failed process
        """
        for member in self.tracker_obj.monitor_list:
            payload = message(FAILURE, proc_id)
            payload = payload.get_encoded_message()
            self.sock.sendto(payload, (member, self.port))

    def pinger(self):
        while True:
            self.ping()
            sleep(PING_TIME)


class introducer:
    def __init__(self, tracker_obj=None):
        """This is responsible for the TCP communication with introducer.

        Args:
            tracker_obj (tracker, optional): This will enable this class to access shared data in tracker. Defaults to None.
        """
        global logger
        self.logger = logger
        self.introducer_hostname = "fa22-cs425-1901"
        self.port = 6000 + get_host_number_from_hostname(HOSTNAME)
        self.tracker_obj = tracker_obj

    def connect_to_introducer(self):
        """Opens socket and connects to introducer"""
        try:
            temp_conn = socket.socket()
            temp_conn.connect((self.introducer_hostname, self.port))
            INTRODUCER_CONN[0] = temp_conn
        except Exception as e:
            self.logger.debug(e)
            print(
                "{}:{} is down!, {}".format(self.introducer_hostname, self.port, str(e))
            )

    def close_conn(self):
        """closes socket"""
        try:
            INTRODUCER_CONN[0].close()
        except Exception as e:
            self.logger.debug(e)

    def send_fail(self, proc_id):
        """This will notify the introducer about a failed process which this is monitoring to introducer

        Args:
            proc_id (str): Process ID of the failed process
        """
        payload = message(FAILURE, proc_id)
        payload = payload.get_encoded_message()
        INTRODUCER_CONN[0].sendall(payload)

    def send_leave(self, proc_id):
        """This will notify the introducer about a left process which this is monitoring to introducer

        Args:
            proc_id (str): Process ID of the process that left
        """
        try:
            payload = message(LEAVE, proc_id)
            payload = payload.get_encoded_message()
            INTRODUCER_CONN[0].sendall(payload)
        except Exception as e:
            print(e)
            self.logger.debug(e)
            pass

    def send_join(self):
        """Sends join request to the introducer"""
        try:
            payload = message(JOIN)
            payload = payload.get_encoded_message()
            INTRODUCER_CONN[0].sendall(payload)
        except Exception as e:
            print("Join Failed: {}".format(e))
            self.logger.debug(e)

    def recv(self):
        """Receives data from introducer"""
        global SELF_PROC_ID
        while True:
            try:
                data = INTRODUCER_CONN[0].recv(BUFFER_SIZE)
                message_obj = message()
                data = message_obj.decode_message(data)
                if data["T"] == JOIN:
                    recvd_list = data["D"]
                    for member in recvd_list:
                        if HOSTNAME in member:
                            SELF_PROC_ID = member
                            continue
                        self.tracker_obj.add_member(member)
                        self.tracker_obj.update_monitor()
            except Exception as e:
                print("Exception in introducer recv: {}".format(e))
                self.connect_to_introducer()


class UI:
    """This class if for UI"""

    def __init__(self, threads, tracker_obj):
        self.tracker_obj = tracker_obj
        self.introducer_obj = introducer(tracker_obj=tracker_obj)
        self.send_obj = sender(tracker_obj=tracker_obj)
        self.threads = threads
        self.print_help()

    def pretty_print(self):
        """Pints membership list"""
        table = PrettyTable()
        table.field_names = ["Process IDs"]
        temp_mem_list = self.tracker_obj.membership_list[:]
        for item in temp_mem_list:
            table.add_row([item])
        print(table)

    def print_help(self):
        print("join to join the system")
        print("list_mem to print membership list")
        print("leave to leave")
        print("list_self to get process id")
        print("help to help")

    def take_input(self):
        """Listens for user input"""

        global SELF_PROC_ID

        while True:
            inp = input("$")
            if inp == "join":
                self.introducer_obj.connect_to_introducer()
                self.introducer_obj.send_join()
                for thread in self.threads:
                    thread.start()
            elif inp == "list_mem":
                self.pretty_print()
            elif inp == "leave":
                self.send_obj.send_leave(SELF_PROC_ID)
                try:
                    for thread in self.threads:
                        thread.kill()
                except:
                    pass
                self.introducer_obj.close_conn()
                exit(0)
            elif inp == "list_self":
                print(SELF_PROC_ID)
            elif inp == "10":
                print(self.tracker_obj.monitor_list)
            else:
                self.print_help()


def main():
    tracker_obj = tracker()
    sender_obj = sender(tracker_obj)
    listner_obj = listner(tracker_obj)
    introducer_obj = introducer(tracker_obj=tracker_obj)
    threads = []
    threads.append(threading.Thread(target=sender_obj.pinger, daemon=True))
    threads.append(threading.Thread(target=listner_obj.recv, daemon=True))
    threads.append(threading.Thread(target=introducer_obj.recv, daemon=True))
    threads.append(threading.Thread(target=tracker_obj.track, daemon=True))

    ui_obj = UI(threads, tracker_obj)
    ui_obj.take_input()

    for thread in threads:
        thread.join()


if __name__ == "__main__":
    main()
