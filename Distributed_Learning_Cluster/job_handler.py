from collections import deque, defaultdict
from message import message
from mp3_constants import *
from mp4_constants import *
from mp2_constants import *
from utils import *
from tcp_connection import tcp_connection
import time


class job_handler:
    def __init__(
        self,
        query_data,
        membership_list,
        self_proc_id,
        metadata,
        job_id,
        port,
        is_restart=False,
    ):
        self.metadata = metadata
        self.membership_list = membership_list
        self.job_id = job_id
        self.port = port
        self.self_proc_id = self_proc_id
        self.query_data = query_data
        self.message_decoder = message()
        self.busy_nodes = defaultdict(lambda: 0)
        if is_restart:
            self.total_files = len(query_data.files)
            return
        query_data.files.extend(self.get_files())
        self.total_files = len(query_data.files)
        # print("assigned nodes: ",self.query_data.assigned_nodes)

    def get_files(self):
        files = deque([])
        normal_file_name, _, _ = get_file_name_ext_version(self.query_data.file_name)
        for proc_id in self.metadata:
            for file_name in self.metadata[proc_id]:
                if (
                    file_name.startswith(normal_file_name)
                    and file_name not in self.query_data.processed_files
                ):
                    files.append(file_name)
        return deque(list(set(files)))

    def check_and_fix(self):
        mem_nodes = [
            get_hostname_from_proc_id(proc_id)
            for proc_id in self.membership_list + [self.self_proc_id]
        ]
        check = False
        for node in list(self.query_data.dispatch_files):
            if node not in mem_nodes:
                files = self.query_data.dispatch_files[node]
                self.query_data.files.extend(files)
                try:
                    self.query_data.assigned_nodes.remove(node)
                except:
                    pass
                # print("deleting node", node, files)
                try:
                    del self.query_data.dispatch_files[node]
                except:
                    pass
                check = True
        return check

    def wait_for_completion(self):
        while self.query_data.count < self.total_files:
            if len(self.query_data.files) > 0:
                self.handle_job()
            if self.check_and_fix():
                print("Fixing the job")
                self.handle_job()
            continue
        self.query_data.status = "COMPLETED"
        self.query_data.assigned_nodes.clear()
        self.query_data.end = time.time()

    def handle_job(self):
        while len(self.query_data.files) > 0:
            # print("files", self.query_data.files)
            self.check_and_fix()
            temp_files = []
            for i in range(self.query_data.batch_size):
                if len(self.query_data.files) > 0:
                    temp_file = self.query_data.files.popleft()
                    temp_files.append(temp_file)
            payload = message(
                INFER,
                {
                    "type": self.query_data.type,
                    "job_id": self.job_id,
                    "files": temp_files,
                    "port": self.port,
                },
            )
            payload = payload.get_encoded_message()
            if len(self.query_data.assigned_nodes) == 0:
                self.query_data.files.extend(temp_files)
                print("No nodes available to process the job")
                continue
            while True:
                node = self.query_data.assigned_nodes.popleft()
                self.query_data.assigned_nodes.append(node)
                if self.busy_nodes[node] == 1:
                    continue
                self.busy_nodes[node] = 1
                break
            try:
                tcp_conn_obj = tcp_connection(node, ML_SERVER_PORT)
                tcp_conn_obj.connect_to_target()
                tcp_conn_obj.conn.sendall(payload)
                tcp_conn_obj.conn.close()
            except Exception as e:
                print("Error in job_handler_handle_job", e)
                self.query_data.files.extend(temp_files)
                continue
            for file_name in temp_files:
                self.query_data.dispatch_files[node].append(file_name)
            # print("during dispatching" ,self.query_data.dispatch_files)
            dispatched_time = time.time()
            # for file_name in temp_files:
            #     self.query_data.dispatch_timestamps[file_name].append(dispatched_time)
        self.wait_for_completion()

    def serve(self):
        try:
            host = HOSTNAME
            port = self.port
            self.server = socket.socket()
            self.server.bind((host, port))
            # It will enable the server object to listen to 10 concurrent queries.
            self.server.listen(10)
            print("Job handler listening on port: {}".format(port))
        except Exception as e:
            print("Job handler Server failed\n{}".format(str(e)))
        self.recv()

    def recv(self):
        """Listens for incoming connections and download requests"""
        while True:
            try:
                if self.query_data.status == "COMPLETED":
                    break
                try:
                    self.conn, self.address = self.server.accept()
                    self.address = self.address[0]
                    recvd_message = self.conn.recv(BUFFER_SIZE)
                    self.conn.close()
                except Exception as e:
                    print("Error in closing job_handler connection: {}".format(e))
                    continue
                # print("qeury_data", self.query_data)
                recvd_message = self.message_decoder.decode_message(recvd_message)
                node = IP_HOSTNAME_MAP[self.address]
                self.busy_nodes[node] = 0
                # print("node: ",node)
                # print("disp files:, ",self.query_data.dispatch_files)
                # print("recvd_message",recvd_message)
                self.query_data.count += len(recvd_message["D"]["files"])
                for file_name in recvd_message["D"]["files"]:
                    time_now = time.time()
                    try:
                        self.query_data.dispatch_files[node].remove(file_name)
                    except:
                        continue
                    self.query_data.processed_files.append(file_name)
                    self.query_data.time_stamps.append(time_now)
                    self.query_data.dispatch_timestamps[file_name].extend(
                        recvd_message["D"]["dispatch_timestamps"][file_name]
                    )
            except Exception as e:
                print("Error in job_handler_recv", e)
