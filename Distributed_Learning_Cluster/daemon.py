import socket
from mp2_constants import *
from message import message
from utils import *
import threading
from time import sleep
from collections import defaultdict
from prettytable import PrettyTable
from mp2_logger import get_logger
from mp4_constants import *
from collections import deque
from file_handler import file_handler
from uploader import uploader
from downloader import downloader
from hasher_handler import hash_handler
from replicator import replicator
from remover import remover
from sequencer import sequencer
from tcp_connection import tcp_connection
from ml_server import ml_server
from ml_coordinator import ml_cord
from appender import appender
import os

os.system("sudo rm sdfs_files/*")
os.system("sudo rm downloads/*")


INTRODUCER_CONN = [None]
SELF_PROC_ID = ""
logger = get_logger("mon_log")
ack_que = deque()


class tracker:
    """This calss is responsible for tacking pings and concurrency control"""

    def __init__(self):
        self.ping_dict = defaultdict(int)
        self.ack_dict = defaultdict(int)
        self.membership_list = []
        self.monitor_list = []
        self.monitor_lock = threading.Lock()
        self.ping_lock = threading.Lock()
        self.ack_lock = threading.Lock()
        self.membership_list_lock = threading.Lock()
        payload = message(PING, "")
        self.introducer_obj = introducer()
        self.payload = payload.get_encoded_message()
        self.sender_obj = sender(self)
        self.replicator_obj = None

    def track(self):
        global SELF_PROC_ID
        while True:
            try:
                self.monitor_lock.acquire()
                self.ping_lock.acquire()
                self.membership_list_lock.acquire()
                self.ack_lock.acquire()
                logger.debug("ping dict: {}".format(self.ping_dict))
                logger.debug("ack dict: {}".format(self.ack_dict))
                down = []
                for member in self.monitor_list:
                    if self.ping_dict[member] - self.ack_dict[member] <= 2:
                        continue
                    down.append(member)
                if len(down) > 0:
                    logger.info("Members down: {}".format(down))
                for member in down:
                    proc_id = get_proc_id_from_hostname(member, self.membership_list)
                    self.remove_member_in_membership_list(proc_id, locked=True)
                    self.update_monitor(locked=True)
                    self.introducer_obj.send_fail(proc_id)
                    self.sender_obj.send_fail(proc_id)
                    self.replicator_obj.check_and_replicate(proc_id)
            except Exception as e:
                # print(e)
                pass
            finally:
                self.monitor_lock.release()
                self.ping_lock.release()
                self.ack_lock.release()
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

    def update_ack(self, member, ack_no):
        """This functions will update the ack_dict with new ack number

        Args:
            member (str): hostname
            ack_no : ack number
        """
        if member not in self.ping_dict:
            return
        self.ack_lock.acquire()
        try:
            self.ack_dict[member] = max(self.ack_dict[member], ack_no)
        finally:
            self.ack_lock.release()

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
            if proc_id in self.membership_list:
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
            self.ack_lock.acquire()
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
                    self.ack_dict[member] = 0
            temp = list(self.ping_dict.keys())
            for member in temp:
                if member not in temp_mon_list:
                    if member in self.ping_dict:
                        del self.ping_dict[member]
                    if member in self.ack_dict:
                        del self.ack_dict[member]
            for member in self.ping_dict:
                self.ping_dict[member] = 0
                self.ack_dict[member] = 0
        finally:
            if locked == False:
                self.ack_lock.release()
                self.ping_lock.release()
                self.membership_list_lock.release()
                self.monitor_lock.release()


class acker:
    def __init__(self, tracker_obj):
        self.queue = deque()
        self.tracker_obj = tracker_obj
        self.lock = threading.Lock()
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.port = 6000

    def send_ack(self):
        """Respond with ACK

        Args:
            address (str): IP of the host to which ACK should be sent
        """
        while True:
            ack_no = None
            self.lock.acquire()
            try:
                # if len(self.queue) > 3:
                #     print("queue: ",self.queue)
                if len(self.queue) != 0:
                    logger.debug(str(self.queue))
                    address, ack_no = self.queue.popleft()
            except Exception as e:
                print("Exception in ack: ", e)
            finally:
                self.lock.release()
            if ack_no is None:
                continue
            payload = message(ACK, ack_no)
            payload = payload.get_encoded_message()
            self.sock.sendto(payload, (address, self.port))
            logger.debug("ack sent to {} {}".format(address, ack_no))
            self.tracker_obj.update_ack(address, ack_no)

    def append(self, addr, ack_no):
        self.lock.acquire()
        try:
            self.queue.append((addr, ack_no))
        finally:
            self.lock.release()


class listner:
    def __init__(self, tracker_obj, acker_obj, replicator_obj):
        """This will listen incoming messages from it's peer

        Args:
            tracker_obj (tracker): This will enable the listner class to access tracker for shared data
        """
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(("0.0.0.0", 6000))
        self.sender_obj = sender(tracker_obj)
        self.introducer_obj = introducer(tracker_obj=tracker_obj)
        self.tracker_obj = tracker_obj
        self.acker_obj = acker_obj
        self.replicator_obj = replicator_obj

    def recv(self):
        """This function will receive messages from its peers and triggers respective functions."""
        global SELF_PROC_ID
        while True:
            recvd_message, addr = self.sock.recvfrom(BUFFER_SIZE)
            addr = addr[0]
            addr = IP_HOSTNAME_MAP[addr]
            message_obj = message()
            recvd_message = message_obj.decode_message(recvd_message)
            if recvd_message["T"] == PING:
                logger.debug("Ping requested by {} {}".format(addr, recvd_message["D"]))
                self.acker_obj.append(addr, recvd_message["D"])
            elif recvd_message["T"] == ACK:
                logger.debug(
                    "ping received from {} {}".format(addr, str(recvd_message["D"]))
                )
                self.tracker_obj.update_ack(addr, recvd_message["D"])
            elif recvd_message["T"] == FAILURE:
                proc_id = recvd_message["D"]
                if proc_id not in self.tracker_obj.membership_list:
                    pass
                else:
                    self.introducer_obj.send_fail(proc_id)
                    self.tracker_obj.remove_member_in_membership_list(proc_id)
                    self.tracker_obj.update_monitor()
                    self.sender_obj.send_fail(proc_id)
                    self.replicator_obj.check_and_replicate(proc_id)
            elif recvd_message["T"] == LEAVE:
                proc_id = recvd_message["D"]
                if proc_id not in self.tracker_obj.membership_list:
                    pass
                else:
                    self.introducer_obj.send_leave(proc_id)
                    self.tracker_obj.remove_member_in_membership_list(proc_id)
                    self.tracker_obj.update_monitor()
                    self.sender_obj.send_leave(proc_id)
                    self.replicator_obj.check_and_replicate(proc_id)


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
            payload = message(PING, self.tracker_obj.ping_dict[member] + 1)
            logger.debug(
                "sending ping to {} {}".format(
                    member, str(self.tracker_obj.ping_dict[member] + 1)
                )
            )
            payload = payload.get_encoded_message()
            self.sock.sendto(payload, (member, self.port))
            self.tracker_obj.inc_ping(member)

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
    def __init__(self, tracker_obj=None, replicator_obj=None):
        """This is responsible for the TCP communication with introducer.

        Args:
            tracker_obj (tracker, optional): This will enable this class to access shared data in tracker. Defaults to None.
        """
        self.introducer_hostname = "introducer"
        self.port = 6000 + get_host_number_from_hostname(HOSTNAME)
        self.tracker_obj = tracker_obj
        self.replicator_obj = replicator_obj

    def connect_to_introducer(self):
        """Opens socket and connects to introducer"""
        try:
            temp_conn = socket.socket()
            temp_conn.connect((self.introducer_hostname, self.port))
            INTRODUCER_CONN[0] = temp_conn
        except Exception as e:
            print(
                "{}:{} is down!, {}".format(self.introducer_hostname, self.port, str(e))
            )

    def close_conn(self):
        """closes socket"""
        try:
            INTRODUCER_CONN[0].close()
        except Exception as e:
            print("Error while closing connection, {}".format(str(e)))
            pass

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
            pass

    def notify_replication(self, payload):
        INTRODUCER_CONN[0].sendall(payload)

    def send_join(self):
        """Sends join request to the introducer"""
        try:
            payload = message(JOIN)
            payload = payload.get_encoded_message()
            INTRODUCER_CONN[0].sendall(payload)
        except Exception as e:
            print("Join Failed: {}".format(e))

    def recv(self):
        """Receives data from introducer"""
        global SELF_PROC_ID
        while True:
            try:
                data = INTRODUCER_CONN[0].recv(BUFFER_SIZE)
                message_obj = message()
                data = message_obj.decode_message(data)
                if data["T"] == JOIN:
                    recvd_data = data["D"]
                    if type(recvd_data) == list:
                        for member in recvd_data:
                            if HOSTNAME in member:
                                SELF_PROC_ID = member
                                continue
                            self.tracker_obj.add_member(member)
                            self.replicator_obj.add_member(member)
                            self.tracker_obj.update_monitor()
                        continue
                    for member in recvd_data["membership_list"]:
                        if HOSTNAME in member:
                            SELF_PROC_ID = member
                            self.replicator_obj.self_proc_id = member
                            continue
                        self.tracker_obj.add_member(member)
                        self.tracker_obj.update_monitor()
                    metadata = recvd_data["metadata"]
                    for key in metadata:
                        self.replicator_obj.metadata[key] = set(metadata[key])
                    self.replicator_obj.download_replicas()
            except Exception as e:
                print("Exception in introducer recv: {}".format(e))
                self.connect_to_introducer()


class UI:
    """This class if for UI"""

    def __init__(self, threads, tracker_obj, acker_obj, replicator_obj):
        self.tracker_obj = tracker_obj
        self.introducer_obj = introducer(tracker_obj=tracker_obj)
        self.send_obj = sender(tracker_obj=tracker_obj)
        self.threads = threads
        self.print_help()
        self.acker_obj = acker_obj
        self.replicator_obj = replicator_obj

    def pretty_print(self):
        """Pints membership list"""
        table = PrettyTable()
        table.field_names = ["Process IDs"]
        temp_mem_list = self.tracker_obj.membership_list[:]
        for item in temp_mem_list:
            table.add_row([item])
        print(table)

    def pretty_print_gen(self, gen_list, column):
        table = PrettyTable()
        table.field_names = [column]
        count = 0
        for item in gen_list:
            if column == "Process IDs":
                if count == 4:
                    break
            count += 1
            table.add_row([item])
        print(table)

    def process_job_info(self):
        try:
            ML_CORD = get_ml_cord(self.tracker_obj.membership_list + [SELF_PROC_ID])
            tcp_conn_obj = tcp_connection(ML_CORD, ML_CORD_PORT)
            payload = message(JOB_INFO, "")
            payload = payload.get_encoded_message()
            tcp_conn_obj.connect_to_target()
            tcp_conn_obj.conn.send(payload)
            recvd_data = tcp_conn_obj.conn.recv(BUFFER_SIZE)
            message_obj = message()
            recvd_data = message_obj.decode_message(recvd_data)
            recvd_data = recvd_data["D"]
            tcp_conn_obj.close_conn()
            table = PrettyTable()
            table.field_names = [
                "Job ID",
                "Job Type",
                "Job Status",
                "Batch_size",
                "Dataset",
            ]
            for job_id in recvd_data:
                if job_id == "buffer":
                    continue
                job = recvd_data[job_id]
                table.add_row(
                    [
                        job_id,
                        job["type"],
                        job["status"],
                        job["batch_size"],
                        job["dataset"],
                    ]
                )
            print(table)
        except Exception as e:
            print("unable to fetch data at the moment")

    def process_job_stats(self):
        try:
            ML_CORD = get_ml_cord(self.tracker_obj.membership_list + [SELF_PROC_ID])
            tcp_conn_obj = tcp_connection(ML_CORD, ML_CORD_PORT)
            payload = message(JOB_STATS, "")
            payload = payload.get_encoded_message()
            tcp_conn_obj.connect_to_target()
            tcp_conn_obj.conn.send(payload)
            recvd_data = tcp_conn_obj.conn.recv(BUFFER_SIZE)
            message_obj = message()
            recvd_data = message_obj.decode_message(recvd_data)
            recvd_data = recvd_data["D"]
            tcp_conn_obj.close_conn()
            table = PrettyTable()
            table.field_names = [
                "Job ID",
                "Count",
                "mean",
                "std",
                "25%",
                "75%",
                "99%",
                "10_sec_rate",
                "total_rate",
                "assigned_nodes",
            ]
            for job_id in recvd_data:
                if job_id == "buffer":
                    continue
                job_stats = recvd_data[job_id]
                table.add_row(
                    [
                        str(job_id),
                        str(job_stats["count"]),
                        str(job_stats["mean"])[:8],
                        str(job_stats["std"])[:8],
                        str(job_stats["25th"])[:8],
                        str(job_stats["75th"])[:8],
                        str(job_stats["99th"])[:8],
                        str(job_stats["10_sec_rate"])[:8],
                        str(job_stats["total_rate"])[:8],
                        str(job_stats["assigned_nodes"]),
                    ]
                )
            print(table)
        except Exception as e:
            print("unable to fetch data at the moment")

    def print_help(self):
        print("join to join the system")
        print("list_mem to print membership list")
        print("leave to leave")
        print("list_self to get process id")
        print("put local_file_name sdfs_file_name to upload a file to sdfs")
        print("get sdfs_file_name local_file_name to download a file from sdfs")
        print("delete sdfs_file_name to delete a file from sdfs")
        print(
            "get-versions sdfs_file_name number_of_versions local_file_name to get the last n versions of a file"
        )
        print("ls sdfs_file_name to list all nodes that have the file")
        print("store to list all files stored in the node")
        print("train")
        print("upload_data dataset count")
        print("infer model dataset batch_size")
        print("job_info to get job info")
        print("job_stats to get job stats")
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
                new_threads = []
                if HOSTNAME in ["fa22-cs425-1901", "fa22-cs425-1902"]:
                    ml_cord_obj = ml_cord(
                        HOSTNAME, self.tracker_obj, self.replicator_obj, SELF_PROC_ID
                    )
                    new_threads.append(
                        threading.Thread(target=ml_cord_obj.serve, daemon=True)
                    )
                    new_threads.append(
                        threading.Thread(
                            target=ml_cord_obj.check_and_restart_jobs, daemon=True
                        )
                    )
                ml_server_obj = ml_server(
                    self.tracker_obj.membership_list, SELF_PROC_ID, self.acker_obj
                )
                new_threads.append(
                    threading.Thread(target=ml_server_obj.serve, daemon=True)
                )
                new_threads.append(
                    threading.Thread(target=ml_server_obj.process_jobs, daemon=True)
                )
                for thread in new_threads:
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
            elif inp[:3] == "put":
                try:
                    local_file_name, sdfs_file_name = inp.split()[1:]
                except:
                    print("Invalid command")
                    continue
                try:
                    file_handler_obj = file_handler(
                        self.tracker_obj.membership_list,
                        local_file_name=local_file_name,
                        sdfs_file_name=sdfs_file_name,
                        SELF_PROC_ID=SELF_PROC_ID,
                        is_uploader=True,
                    )
                    done = file_handler_obj.upload_file()
                    if done == 1:
                        print("File uploaded successfully")
                    else:
                        print("File upload failed!")
                except Exception as e:
                    print("File upload exception: {}".format(str(e)))
            elif inp.startswith("job_info"):
                self.process_job_info()
            elif inp.startswith("job_stats"):
                try:
                    # job_id = inp.split()[1]
                    pass
                except:
                    print("Invalid command")
                    continue
                self.process_job_stats()
            elif inp.startswith("get-versions"):
                try:
                    sdfs_file_name, number_of_versions, local_file_name = inp.split()[
                        1:
                    ]
                except:
                    print("Invalid command")
                    continue
                try:
                    if not check_if_file_exists_in_metadata(
                        sdfs_file_name, self.replicator_obj.metadata
                    ):
                        print("File does not exist")
                        continue
                    file_handler_obj = file_handler(
                        self.tracker_obj.membership_list,
                        local_file_name=local_file_name,
                        sdfs_file_name=sdfs_file_name,
                        SELF_PROC_ID=SELF_PROC_ID,
                        acker_obj=self.acker_obj,
                    )
                    done = file_handler_obj.download_versions(
                        int(number_of_versions), self.replicator_obj.metadata
                    )
                    if done == 1:
                        print("File downloaded successfully")
                    else:
                        print("File download failed!")
                except Exception as e:
                    print("File download exception: {}".format(e))
            elif inp[:3] == "get":
                try:
                    sdfs_file_name, local_file_name = inp.split()[1:]
                except:
                    print("Invalid command")
                    continue
                try:
                    if not check_if_file_exists_in_metadata(
                        sdfs_file_name, self.replicator_obj.metadata
                    ):
                        print("File does not exist")
                        continue
                    file_handler_obj = file_handler(
                        self.tracker_obj.membership_list,
                        local_file_name=local_file_name,
                        sdfs_file_name=sdfs_file_name,
                        SELF_PROC_ID=SELF_PROC_ID,
                        acker_obj=self.acker_obj,
                    )
                    done = file_handler_obj.download_file()
                    if done == 1:
                        print("File downloaded successfully")
                    else:
                        print("File download failed!")
                except Exception as e:
                    print("File download exception: {}".format(e))
            elif inp.startswith("delete"):
                try:
                    sdfs_file_name = inp.split()[1]
                except:
                    print("Invalid command")
                    continue
                try:
                    file_handler_obj = file_handler(
                        self.tracker_obj.membership_list,
                        local_file_name=None,
                        sdfs_file_name=sdfs_file_name,
                        SELF_PROC_ID=SELF_PROC_ID,
                        acker_obj=self.acker_obj,
                    )
                    done = file_handler_obj.delete_file()
                    if done == 1:
                        print("File deleted successfully")
                    else:
                        print("File delete failed!")
                except Exception as e:
                    print("File delete exception: {}".format(e))
            elif inp.startswith("ls"):
                try:
                    sdfs_file_name = inp.split()[1]
                except:
                    print("Invalid command")
                    continue
                try:
                    normal_file_name = get_file_name_ext_version(sdfs_file_name)[0]
                    self.pretty_print_gen(
                        get_proc_ids_of_sdfs_file(
                            normal_file_name, self.replicator_obj.metadata
                        ),
                        "Process IDs",
                    )
                except Exception as e:
                    print("File list exception: {}".format(e))
            elif inp.startswith("train"):
                try:
                    local_file_name = "lenet_model.h5"
                    sdfs_file_name = "lenet_model.h5"
                    file_handler_obj = file_handler(
                        self.tracker_obj.membership_list,
                        local_file_name=local_file_name,
                        sdfs_file_name=sdfs_file_name,
                        SELF_PROC_ID=SELF_PROC_ID,
                        is_uploader=True,
                    )
                    done = file_handler_obj.upload_file()
                    if done == 1:
                        print("lenet trained successfully")
                    else:
                        print("lenet failed!")
                except Exception as e:
                    print("lenet upload exception: {}".format(str(e)))
                try:
                    local_file_name = "resnet_model.h5"
                    sdfs_file_name = "resnet_model.h5"
                    file_handler_obj = file_handler(
                        self.tracker_obj.membership_list,
                        local_file_name=local_file_name,
                        sdfs_file_name=sdfs_file_name,
                        SELF_PROC_ID=SELF_PROC_ID,
                        is_uploader=True,
                    )
                    done = file_handler_obj.upload_file()
                    if done == 1:
                        print("resnet trained successfully")
                    else:
                        print("resnet failed!")
                except Exception as e:
                    print("resnet upload exception: {}".format(str(e)))
            elif inp.startswith("upload_data"):
                try:
                    dataset, count = inp.split()[1], inp.split()[2]
                except:
                    print("Invalid command")
                    continue
                try:
                    files = get_all_files_for_dataset(dataset)
                    normal_file_name, ext, _ = get_file_name_ext_version(dataset)
                    count = int(count)
                    done = 0
                    i = 1
                    for i in range(count):
                        file = files[i % len(files)]
                        local_file_name = file
                        sdfs_file_name = normal_file_name + str(i) + ext
                        file_handler_obj = file_handler(
                            self.tracker_obj.membership_list,
                            local_file_name=local_file_name,
                            sdfs_file_name=sdfs_file_name,
                            SELF_PROC_ID=SELF_PROC_ID,
                            is_uploader=True,
                        )
                        done += file_handler_obj.upload_file()
                    if done == count:
                        print("dataset uploaded successfully")
                    else:
                        print("dataset upload failed!")
                except Exception as e:
                    print("File upload exception: {}".format(str(e)))
            elif inp.startswith("infer"):
                try:
                    model, sdfs_file_name, batch_size = (
                        inp.split()[1],
                        inp.split()[2],
                        inp.split()[3],
                    )
                except:
                    print("Invalid command")
                    continue
                ML_CORD = get_ml_cord(self.tracker_obj.membership_list + [SELF_PROC_ID])
                tcp_conn_obj = tcp_connection(ML_CORD, ML_CORD_PORT)
                payload = message(
                    JOB,
                    {
                        "type": model,
                        "sdfs_file_name": sdfs_file_name,
                        "batch_size": batch_size,
                    },
                )
                payload = payload.get_encoded_message()
                tcp_conn_obj.connect_to_target()
                tcp_conn_obj.conn.send(payload)
                tcp_conn_obj.close_conn()
                print("Inference job submitted")
            elif inp.startswith("tail"):
                try:
                    job_id, count = inp.split()[1], inp.split()[2]
                except:
                    print("Invalid command")
                    continue
                sdfs_file_name, local_file_name = job_id + ".csv", job_id + ".csv"
                try:
                    if not check_if_file_exists_in_metadata(
                        sdfs_file_name, self.replicator_obj.metadata
                    ):
                        print("File does not exist")
                        continue
                    file_handler_obj = file_handler(
                        self.tracker_obj.membership_list,
                        local_file_name=local_file_name,
                        sdfs_file_name=sdfs_file_name,
                        SELF_PROC_ID=SELF_PROC_ID,
                        acker_obj=self.acker_obj,
                    )
                    done = file_handler_obj.download_file()
                    if done == 1:
                        with open("downloads/" + local_file_name, "r") as f:
                            lines = f.readlines()
                            print("".join(lines[-int(count) :]))
                    else:
                        print("File download failed!")
                except Exception as e:
                    print("File read exception: {}".format(e))
            elif inp.startswith("store"):
                try:
                    self.pretty_print_gen(
                        store(self.replicator_obj.metadata, SELF_PROC_ID),
                        "SDFS File Names",
                    )
                except Exception as e:
                    print("Store exception: {}".format(e))
            elif inp == "10":
                print(self.tracker_obj.monitor_list)
            elif inp == "11":
                print(self.replicator_obj.metadata)
            else:
                self.print_help()


def main():
    global SELF_PROC_ID
    tracker_obj = tracker()
    acker_obj = acker(tracker_obj)
    replicator_obj = replicator(tracker_obj.membership_list, acker_obj)
    introducer_obj = introducer(tracker_obj=tracker_obj, replicator_obj=replicator_obj)
    tracker_obj.replicator_obj = replicator_obj
    replicator_obj.introducer_obj = introducer_obj
    sender_obj = sender(tracker_obj)
    sequencer_obj_processor = sequencer(replicator_obj.metadata)
    sequencer_obj_server = sequencer(replicator_obj.metadata)
    sequencer_obj_server.sequence = sequencer_obj_processor.sequence
    listner_obj = listner(tracker_obj, acker_obj, replicator_obj)
    appender_obj = appender(
        sdfs_file_name=None,
        target=None,
        counter=[],
        membership_list=tracker_obj.membership_list,
        introducer_obj=introducer_obj,
        is_server=True,
        results=None,
    )
    uploader_obj = uploader(
        local_file_name=None,
        sdfs_file_name=None,
        target=None,
        counter=[],
        membership_list=tracker_obj.membership_list,
        introducer_obj=introducer_obj,
        is_server=True,
    )
    downloader_obj = downloader(
        local_file_name=None,
        sdfs_file_name=None,
        node=None,
        acker_obj=acker_obj,
        is_server=True,
    )
    hash_obj = hash_handler()
    remover_obj = remover(None, None, tracker_obj.membership_list, introducer_obj, True)
    threads = []
    threads.append(threading.Thread(target=sender_obj.pinger, daemon=True))
    threads.append(threading.Thread(target=listner_obj.recv, daemon=True))
    threads.append(threading.Thread(target=introducer_obj.recv, daemon=True))
    threads.append(threading.Thread(target=acker_obj.send_ack, daemon=True))
    threads.append(threading.Thread(target=acker_obj.send_ack, daemon=True))
    threads.append(threading.Thread(target=acker_obj.send_ack, daemon=True))
    threads.append(threading.Thread(target=tracker_obj.track, daemon=True))
    threads.append(threading.Thread(target=uploader_obj.serve, daemon=True))
    threads.append(threading.Thread(target=appender_obj.serve, daemon=True))
    threads.append(threading.Thread(target=downloader_obj.serve, daemon=True))
    threads.append(threading.Thread(target=sequencer_obj_server.serve, daemon=True))
    threads.append(threading.Thread(target=replicator_obj.serve, daemon=True))
    threads.append(threading.Thread(target=hash_obj.serve, daemon=True))
    threads.append(threading.Thread(target=remover_obj.serve, daemon=True))
    threads.append(
        threading.Thread(target=sequencer_obj_processor.process_metadata, daemon=True)
    )
    ui_obj = UI(threads, tracker_obj, acker_obj, replicator_obj)
    ui_obj.take_input()

    for thread in threads:
        thread.join()


if __name__ == "__main__":
    main()
