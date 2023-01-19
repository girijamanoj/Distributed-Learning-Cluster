from tcp_connection import tcp_connection
from mp2_constants import *
from mp3_constants import *
from utils import *
from message import message
from collections import defaultdict
from file_handler import file_handler
from hasher_handler import hash_handler
from uploader import uploader


class replicator:
    """Replicates the files in the SDFS in case of failure of a node or join of a new node"""

    def __init__(self, membership_list, acker_obj):
        self.metadata = defaultdict(set)
        self.port = 10000
        self.message_decoder = message()
        self.membership_list = membership_list
        self.hash_handler = hash_handler()
        self.introducer_obj = None
        self.self_proc_id = None
        self.acker_obj = acker_obj
        self.ping_lock = None

    def add_member(self, proc_id):
        """Adds a new member to the membership list"""
        self.metadata[proc_id] = set()

    def serve(self):
        """Serves the files to the requesting nodes"""
        try:
            host = HOSTNAME
            port = self.port
            self.server = socket.socket()
            self.server.bind((host, port))
            # It will enable the server object to listen to 10 concurrent queries.
            self.server.listen(10)

        except Exception as e:
            # self.logger.debug(ip, str(e))
            print("Server failed\n{}".format(str(e)))
        self.recv()

    def recv(self):
        """Listens for incoming connections and download requests"""
        while True:
            try:
                conn, address = self.server.accept()
                address = address[0]
                recvd_message = conn.recv(FILE_NAME_SIZE)
                recvd_message = self.message_decoder.decode_message(recvd_message)
                file_name = recvd_message["D"].strip()
                host_name = IP_HOSTNAME_MAP[address]
                if recvd_message["T"] == REPLICATOR:
                    self.metadata[
                        get_proc_id_from_hostname(
                            host_name, self.membership_list + [self.self_proc_id]
                        )
                    ].add(file_name)
                elif recvd_message["T"] == REMOVE:
                    normal_file_name, extension, version = get_file_name_ext_version(
                        file_name
                    )
                    for meta_file_name in list(
                        self.metadata[
                            get_proc_id_from_hostname(
                                host_name, self.membership_list + [self.self_proc_id]
                            )
                        ]
                    ):
                        if meta_file_name.startswith(normal_file_name):
                            self.metadata[
                                get_proc_id_from_hostname(
                                    host_name,
                                    self.membership_list + [self.self_proc_id],
                                )
                            ].remove(meta_file_name)
                            self.notify_nodes(normal_file_name + extension, True)
                elif recvd_message["T"] == REMOVE_ONLY:
                    normal_file_name, extension, version = get_file_name_ext_version(
                        file_name
                    )
                    self.check_and_update(normal_file_name, extension)
                elif recvd_message["T"] == DELETE:
                    normal_file_name, extension, version = get_file_name_ext_version(
                        file_name
                    )
                    self.check_and_delete(normal_file_name, extension)
            except Exception as e:
                print(e)
                pass

    def check_and_delete(self, remove_file_name, extension):
        """Checks if the file is present in the SDFS and deletes it"""
        present = False
        for meta_file_name in list(self.metadata[self.self_proc_id]):
            if meta_file_name.startswith(remove_file_name):
                self.metadata[self.self_proc_id].remove(meta_file_name)
                present = True
        for proc_id in self.metadata:
            for meta_file_name in list(self.metadata[proc_id]):
                if meta_file_name.startswith(remove_file_name):
                    self.metadata[proc_id].remove(meta_file_name)
        if present:
            files = get_all_versions(remove_file_name)
            for file in files:
                os.remove("sdfs_files/" + file)

    def check_and_update(self, remove_file_name, extension):
        """Checks if the file is present in the SDFS and updates the metadata"""
        total_nodes = set()
        new_nodes = self.get_target_nodes(
            remove_file_name + extension, self.membership_list, self.self_proc_id
        )
        for proc_id in self.metadata:
            for file_name in self.metadata[proc_id]:
                if file_name.startswith(remove_file_name):
                    total_nodes.add(get_hostname_from_proc_id(proc_id))
        redundant_nodes = total_nodes - new_nodes
        for node in redundant_nodes:
            proc_id = get_proc_id_from_hostname(
                node, self.membership_list + [self.self_proc_id]
            )
            for meta_file_name in list(self.metadata[proc_id]):
                if meta_file_name.startswith(remove_file_name):
                    self.metadata[proc_id].remove(meta_file_name)
        if get_hostname_from_proc_id(self.self_proc_id) in redundant_nodes:
            payload = message(REMOVE, remove_file_name + extension)
            payload = payload.get_encoded_message()
            self.introducer_obj.notify_replication(payload)

    def get_target_nodes(self, file_name, membership_list, SELF_PROC_ID):
        """Returns the target nodes for the file"""
        try:
            back = ""
            if "_v" in file_name:
                back = str(file_name)
                file_name = (
                    file_name.split("_v")[0]
                    + "."
                    + self.file_name.split("_v")[1].split(".")[1]
                )
            c = sum([ord(i) for i in file_name])
            ret = set()
            temp_list = membership_list + [SELF_PROC_ID]
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
                file_name = back
            return ret
        except Exception as e:
            print("Exception in get_target_node: {}".format(e))

    def get_sdfs_file_name(self, file_name):
        """Returns the SDFS file name"""
        file_name, extension, version = get_file_name_ext_version(file_name)
        present = check_if_file_exists(file_name)
        if present:
            last_file_name = get_latest_version(file_name)
            ind = last_file_name.rfind(".")
            version = int(last_file_name[ind - 1 : ind])
            version += 1
        file_name = file_name + "_v" + str(version)

    def get_max_file_version(self, file_name, seen_file_name):
        """Returns the max file version"""
        _, _, new_v = get_file_name_ext_version(file_name)
        _, _, old_v = get_file_name_ext_version(seen_file_name)
        if new_v > old_v:
            return file_name, seen_file_name
        return seen_file_name, file_name

    def check_if_file_is_seen(self, normal_file_name, seen):
        """Checks if the file is seen"""
        for seen_file_name in seen:
            if normal_file_name in seen_file_name:
                return seen_file_name
        return None

    def download_replicas(self):
        """Downloads the replicas from the SDFS as this node is a new node"""
        node = get_hostname_from_proc_id(self.self_proc_id)
        seen = set()
        for prod_id in list(self.metadata):
            for file_name in list(self.metadata[prod_id]):
                if file_name in seen:
                    continue
                seen.add(file_name)
                normal_file_name = get_file_name_from_sdfs_file_name(file_name)
                target_nodes = self.get_target_nodes(
                    normal_file_name, self.membership_list, self.self_proc_id
                )
                if node not in target_nodes:
                    continue
                file_handler_obj = file_handler(
                    self.membership_list,
                    file_name,
                    normal_file_name,
                    self.self_proc_id,
                    True,
                    self.acker_obj,
                )
                file_handler_obj.download_file()
                self.notify_nodes(file_name, False)
                base_file_name, extension, version = get_file_name_ext_version(
                    normal_file_name
                )
                self.check_and_update(base_file_name, extension)
                for proc_id in self.membership_list:
                    node = get_hostname_from_proc_id(proc_id)
                    self.send_remove(node, normal_file_name)

    def send_remove(self, node, file_name):
        """Sends the remove message to the node"""
        try:
            payload = message(REMOVE_ONLY, adjust_file_name(file_name, REP=True))
            payload = payload.get_encoded_message()
            tcp_connection_obj = tcp_connection(node, REPLICATOR_PORT)
            tcp_connection_obj.connect_to_target()
            tcp_connection_obj.conn.sendall(payload)
            tcp_connection_obj.close_conn()
        except Exception as e:
            pass

    def get_other_nodes_for_file(self, file_name, failed_process_id):
        """Returns the other nodes for the file"""
        ret = set()
        for proc_id in self.metadata:
            if proc_id is None:
                continue
            if proc_id == failed_process_id:
                continue
            for meta_file_name in self.metadata[proc_id]:
                if meta_file_name == file_name:
                    ret.add(get_hostname_from_proc_id(proc_id))
                if len(ret) == 4:
                    break
        return ret

    def upload_replica(self, file_name, target_node, counter):
        uploader_obj = uploader(
            file_name,
            file_name,
            target_node,
            counter,
            self.membership_list,
            self.introducer_obj,
            is_replicator=True,
        )
        uploader_obj.upload()
        if len(counter) == 1:
            return 1
        self.upload_replica(file_name, target_node, counter)

    def check_and_replicate(self, failed_proc_id):
        return
        """Checks if the file needs to be replicated"""
        if failed_proc_id is None:
            return
        seen = set()
        node = get_hostname_from_proc_id(self.self_proc_id)
        for meta_file_name in list(self.metadata[failed_proc_id]):
            if meta_file_name in seen:
                continue
            seen.add(meta_file_name)
            if meta_file_name in self.metadata[self.self_proc_id]:
                all_nodes = sorted(
                    list(self.get_other_nodes_for_file(meta_file_name, failed_proc_id))
                )
                if node not in all_nodes[:4]:
                    continue
                normal_file_name = get_file_name_from_sdfs_file_name(meta_file_name)
                target_nodes = self.get_target_nodes_for_repl_fail(
                    normal_file_name, self.membership_list, self.self_proc_id
                )
                self.remove_list(target_nodes, all_nodes)
                counter = []
                for target in target_nodes:
                    try:
                        if self.upload_replica(meta_file_name, target, counter):
                            break
                    except Exception as e:
                        # print("Exception in check_and_replicate: {}".format(e))
                        continue
        try:
            del self.metadata[failed_proc_id]
        except Exception as e:
            pass

    def remove_list(self, target_nodes, all_nodes):
        for node in all_nodes:
            if node in target_nodes:
                target_nodes.remove(node)

    def get_target_nodes_for_repl_fail(self, file_name, membership_list, SELF_PROC_ID):
        """Returns the target nodes for the file"""
        try:
            back = ""
            if "_v" in file_name:
                back = str(file_name)
                file_name = (
                    file_name.split("_v")[0]
                    + "."
                    + self.file_name.split("_v")[1].split(".")[1]
                )
            c = sum([ord(i) for i in file_name])
            ret = []
            temp_list = membership_list
            nodes = sorted([get_hostname_from_proc_id(i) for i in temp_list])
            c = c % 10 + 1
            count = 0
            seen = set()
            while count < 12:
                if get_hostname_from_host_num(c) in seen:
                    break
                if get_hostname_from_host_num(c) in nodes:
                    ret.append(get_hostname_from_host_num(c))
                seen.add(get_hostname_from_host_num(c))
                c = c % 10 + 1
                count += 1
            if "_v" in back:
                file_name = back
            return ret
        except Exception as e:
            print("Exception in get_target_node: {}".format(e))

    def notify_nodes(self, file_name, is_remove=False):
        """Notifies the nodes about the file"""
        try:
            adjusted_file_name = adjust_file_name(file_name, REP=True)
            payload = message(REPLICATOR, adjusted_file_name)
            if is_remove:
                payload = message(REMOVE, adjusted_file_name)
            payload = payload.get_encoded_message()
            for node in self.membership_list:
                try:
                    tcp_connection_obj = tcp_connection(
                        get_hostname_from_proc_id(node), REPLICATOR_PORT
                    )
                    tcp_connection_obj.connect_to_target()
                    tcp_connection_obj.conn.sendall(payload)
                    tcp_connection_obj.close_conn()
                except Exception as e:
                    # print("Exception in notify_nodes: {}".format(e))
                    pass
            try:
                self.introducer_obj.notify_replication(payload)
            except Exception as e:
                pass
            try:
                tcp_connection_obj = tcp_connection(HOSTNAME, REPLICATOR_PORT)
                tcp_connection_obj.connect_to_target()
                tcp_connection_obj.conn.sendall(payload)
                tcp_connection_obj.close_conn()
            except Exception as e:
                pass
        except Exception as e:
            # print("Error in notifying nodes {}".format(e))
            pass
