from uploader import uploader
import threading
from hasher_handler import hash_handler
from pythonping import ping
from downloader import downloader
from remover import remover
from utils import *
from message import message
from mp3_constants import *
from tcp_connection import tcp_connection
from mp2_constants import *


class file_handler:
    """Generic class to handle file operations"""

    def __init__(
        self,
        membership_list,
        local_file_name,
        sdfs_file_name,
        SELF_PROC_ID=None,
        replicator=False,
        acker_obj=None,
        is_uploader=False,
    ):
        self.membership_list = membership_list
        self.local_file_name = local_file_name
        self.sdfs_file_name = sdfs_file_name
        self.data = None
        self.self_proc_id = SELF_PROC_ID
        self.hash_handler = hash_handler()
        self.replicator = replicator
        self.acker_obj = acker_obj
        self.message_decoder = message()
        if is_uploader:
            self.get_counter()
            self.insert_version()

    def get_target_nodes(self, rep=4):
        """Returns the nodes where the file should be uploaded"""
        try:
            back = ""
            if "_v" in self.sdfs_file_name:
                back = str(self.sdfs_file_name)
                self.sdfs_file_name = (
                    self.sdfs_file_name.split("_v")[0]
                    + "."
                    + self.sdfs_file_name.split("_v")[1].split(".")[1]
                )
            c = sum([ord(i) for i in self.sdfs_file_name])
            ret = set()
            if not self.replicator:
                temp_list = self.membership_list + [self.self_proc_id]
            else:
                temp_list = self.membership_list
            nodes = sorted([get_hostname_from_proc_id(i) for i in temp_list])
            c = c % 10 + 1
            count = 0
            while count < 12:
                if get_hostname_from_host_num(c) in nodes:
                    ret.add(get_hostname_from_host_num(c))
                c = c % 10 + 1
                if len(ret) == 4:
                    break
                count += 1
            if "_v" in back:
                self.sdfs_file_name = back
            return ret
        except Exception as e:
            print("Exception in get_target_node: {}".format(e))

    def upload_file(self, rep=4):
        """Uploads the file to the SDFS"""
        if not check_if_file_exists(self.local_file_name, is_uploader=True):
            print("File does not exist")
            return 0
        try:
            counter = []
            self.parallel_upload_threads = []
            nodes = self.get_target_nodes(rep)
            if len(nodes) == 0:
                return 0
            for node in nodes:
                temp_obj = uploader(
                    self.local_file_name,
                    self.sdfs_file_name,
                    node,
                    counter,
                    self.membership_list,
                )
                thread = threading.Thread(target=temp_obj.upload, daemon=True)
                self.parallel_upload_threads.append(thread)
            for thread in self.parallel_upload_threads:
                thread.start()

            for thread in self.parallel_upload_threads:
                thread.join()

            if len(counter) == len(nodes):
                return 1
            self.upload_file(rep=len(nodes) - len(counter))
        except Exception as e:
            print(e)

    def ping_test(self, nodes):
        """Pings the nodes and returns the node with the lowest latency"""
        try:
            maxxT, Tnode = None, None
            for node in nodes:
                ping_data = ping(target=node, count=3, timeout=1)
                if maxxT is None:
                    maxxT = ping_data.rtt_avg_ms
                    Tnode = node
                elif maxxT is not None and maxxT > ping_data.rtt_avg_ms:
                    maxxT = ping_data.rtt_avg_ms
                    Tnode = node
            return Tnode
        except Exception as e:
            print("Exception in ping_test: {}".format(str(e)))
            return nodes[0]

    def get_hash_node(self):
        """Returns the node where the file is present"""
        self.hashes = []
        nodes = self.get_target_nodes()
        if len(nodes) == 0:
            return 0, 0
        remote_hash, nodes = self.hash_handler.get_remote_hash(
            self.sdfs_file_name, nodes
        )
        node = self.ping_test(nodes)
        return remote_hash, node

    def download_file(self):
        """Downloads the file from the SDFS"""
        nodes = self.get_target_nodes()
        for i in range(4):
            node = self.ping_test(nodes)
            try:
                download_obj = downloader(
                    self.sdfs_file_name,
                    self.local_file_name,
                    get_hostname_from_proc_id(node),
                    self.acker_obj,
                    self.replicator,
                )
                download_obj.download()
                return 1
            except Exception as e:
                print("Exception in download_file line 90: {}".format(str(e)))
                print("Trying again")
                try:
                    nodes.remove(node)
                except Exception as e:
                    print("no node to remove")
        return 0

    def get_sequencer_conn(self):
        """Returns the connection to the sequencer"""
        nodes = [get_hostname_from_proc_id(node) for node in self.membership_list] + [
            HOSTNAME
        ]
        nodes.sort()
        sequencer = nodes[0]
        try:
            tcp_connection_obj = tcp_connection(sequencer, SEQUENCER_PORT)
            tcp_connection_obj.connect_to_target()
            return tcp_connection_obj
        except Exception as e:
            print("Error in getting sequencer connection: {}".format(e))

    def insert_version(self):
        """Inserts the version of the file in the name"""
        temp = self.sdfs_file_name.split(".")
        self.sdfs_file_name = temp[0] + "_v" + str(self.seq_no) + "." + temp[1]

    def get_counter(self):
        """Gets the sequence count for the file"""
        seq_conn = self.get_sequencer_conn()
        payload = message(SEQUENCER, adjust_file_name(self.sdfs_file_name, True))
        payload = payload.get_encoded_message()
        seq_conn.conn.sendall(payload)
        recvd_message = seq_conn.conn.recv(FILE_NAME_SIZE)
        recvd_message = self.message_decoder.decode_message(recvd_message)
        self.seq_no = recvd_message["D"].strip()
        seq_conn.close_conn()

    def download_versions(self, n, metadata):
        """Downloads the n latest versions of the file"""
        try:
            normal_file_name, _, _ = get_file_name_ext_version(self.sdfs_file_name)
            file_new_versions = get_all_versions_from_metadata(
                normal_file_name, metadata
            )[::-1]
            available_versions = file_new_versions[:n]
            loc_normal_file_name, ext, _ = get_file_name_ext_version(
                self.local_file_name
            )
            for version in available_versions:
                self.sdfs_file_name = version
                sdfs_version = version.split(".")[0].split("_")[-1]
                self.local_file_name = loc_normal_file_name + "_" + sdfs_version + ext
                if self.download_file() == 0:
                    print("Error in downloading file")
                    return 0
            return 1
        except Exception as e:
            print("Exception in download_versions: {}".format(str(e)))
            return 0

    def delete_file(self):
        """Deletes the file from the SDFS"""
        try:
            temp_obj = remover(
                self.sdfs_file_name,
                self.membership_list + [self.self_proc_id],
                self.self_proc_id,
            )
            temp_obj.remove()
            return 1
        except Exception as e:
            print("Exception in delete_file: {}".format(str(e)))
            return 0
