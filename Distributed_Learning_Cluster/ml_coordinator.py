from mp2_constants import *
from message import message
from mp3_constants import *
from mp4_constants import *
from collections import deque
from utils import *
import threading
import time
from job_handler import job_handler
import numpy as np
from collections import defaultdict
from time import sleep
from tcp_connection import tcp_connection


class query_data_class:
    def __init__(self, job_id, type, batch_size, file_name):
        """contains job information

        Args:
            job_id (int): job_id
            type (str): type of job
            batch_size (int): size of batch
            file_name (str): dataset file name
        """
        self.job_id = job_id
        self.status = "IN_PROGRESS"
        self.count = 0
        self.type = type
        self.processed_files = []
        self.time_stamps = []
        self.batch_size = batch_size
        self.file_name = file_name
        self.dispatch_files = defaultdict(list)
        self.dispatch_timestamps = defaultdict(list)
        self.assigned_nodes = deque([])
        self.start = None
        self.end = None
        self.files = deque([])


class ml_cord:
    def __init__(self, hostname, tracker_obj, replicator_obj, self_proc_id):
        """ML coordinator class, responsible for distributing jobs to nodes

        Args:
            hostname (str): hostname of the coordinator
            tracker_obj (tracker): tracker object
            replicator_obj (treplicator): replicator object
            self_proc_id (str): process id of the coordinator
        """
        self.lock = threading.Lock()
        self.hostname = hostname
        self.job_queue = deque([])
        self.message_decoder = message()
        self.jobs_data = {}
        self.metadata = replicator_obj.metadata
        self.membership_list = tracker_obj.membership_list
        self.self_proc_id = self_proc_id
        self.job_id = 0
        self.base_port = 15000
        self.back_start = False

    def serve(self):
        try:
            host = self.hostname
            port = ML_CORD_PORT
            self.server = socket.socket()
            self.server.bind((host, port))
            # It will enable the server object to listen to 10 concurrent queries.
            self.server.listen(10)
        except Exception as e:
            print("Server failed\n{}".format(str(e)))
        self.recv()

    def get_active_count(self):
        count = 0
        for job_id in self.jobs_data:
            if self.jobs_data[job_id].status == "IN_PROGRESS":
                count += 1
        return count

    def get_assigned_nodes(self):
        """get assigned nodes"""
        members = deque(self.membership_list + [self.self_proc_id])
        split = max(1, len(members) // self.get_active_count())
        for job in self.jobs_data:
            if self.jobs_data[job].status == "COMPLETED":
                continue
            assigned_nodes = deque([])
            for i in range(split):
                temp = members.popleft()
                assigned_nodes.append(get_hostname_from_proc_id(temp))
                members.append(temp)
            self.jobs_data[job].assigned_nodes.clear()
            self.jobs_data[job].assigned_nodes.extend(assigned_nodes)
            # print(self.jobs_data[job].assigned_nodes)

    def balance_jobs(self):
        """balance jobs between nodes"""
        max_job_id = -1
        max_job_rate = -1
        min_job_id = -1
        min_job_rate = float("inf")
        for job in self.jobs_data:
            if self.jobs_data[job].status == "COMPLETED":
                continue
            # if time.time() - self.jobs_data[job].start<15: continue
            query_data = self.get_total_stats(job)
            if query_data["total_rate"] == 0:
                continue
            if query_data["total_rate"] > max_job_rate:
                max_job_id = job
                max_job_rate = query_data["total_rate"]
            if query_data["total_rate"] < min_job_rate:
                min_job_id = job
                min_job_rate = query_data["total_rate"]
        if min_job_id != -1 and max_job_rate > 1.5 * min_job_rate:
            self.balance_jobs_between_two_jobs(max_job_id, min_job_id)
        if min_job_id != -1 and max_job_rate > 1.1 * min_job_rate:
            self.balance_jobs_between_two_jobs(max_job_id, min_job_id)

    def balance_jobs_between_two_jobs(self, max_job_id, min_job_id):
        """balance jobs between two jobs

        Args:
            max_job_id (str): job id of the job with max rate
            min_job_id (str): job id of the job with min rate
        """
        max_job = self.jobs_data[max_job_id]
        min_job = self.jobs_data[min_job_id]
        if len(max_job.assigned_nodes) <= 1:
            return
        min_job.assigned_nodes.append(max_job.assigned_nodes.pop())

    def recv(self):
        """Listens for incoming connections and download requests"""
        while True:
            # self.check_and_restart_jobs()
            try:
                try:
                    self.conn, self.address = self.server.accept()
                except Exception as e:
                    print("ML CORD Server failed\n{}".format(str(e)))
                    break
                    continue
                self.address = self.address[0]
                try:
                    recvd_message = self.conn.recv(BUFFER_SIZE * 4)
                except Exception as e:
                    try:
                        self.conn.close()
                    except:
                        pass
                    continue
                try:
                    recvd_message = self.message_decoder.decode_message(recvd_message)
                except Exception as e:
                    continue
                if recvd_message["T"] == JOB:
                    data = recvd_message["D"]
                    self.jobs_data[self.job_id] = query_data_class(
                        self.job_id,
                        data["type"],
                        int(data["batch_size"]),
                        data["sdfs_file_name"],
                    )
                    query_data = self.jobs_data[self.job_id]
                    self.get_assigned_nodes()
                    try:
                        job_handler_obj = job_handler(
                            query_data,
                            self.membership_list,
                            self.self_proc_id,
                            self.metadata,
                            self.job_id,
                            self.base_port + self.job_id,
                        )
                        thread = threading.Thread(target=job_handler_obj.handle_job)
                        query_data.start = time.time()
                        thread.start()
                        thread = threading.Thread(target=job_handler_obj.serve)
                        thread.start()
                    except Exception as e:
                        print("Error in creating job handler ", e)
                    self.job_id += 1
                    self.base_port += 1
                elif recvd_message["T"] == QUERY_DATA:
                    self.jobs_data_back = recvd_message["D"]
                elif recvd_message["T"] == JOB_INFO:
                    payload = message(JOB_INFO, self.get_all_jobs_info())
                    payload = payload.get_encoded_message()
                    self.conn.sendall(payload)
                elif recvd_message["T"] == JOB_STATS:
                    # job_id = int(recvd_message['D'])
                    payload = message(JOB_STATS, self.get_all_jobs_stats())
                    payload = payload.get_encoded_message()
                    self.conn.sendall(payload)
                try:
                    self.conn.close()
                except Exception as e:
                    print("Error in closing ml_cord connection: {}".format(e))
            except Exception as e:
                print("Error in ml_cord", e)

    def convert_query_data_to_list(self):
        """convert query data to list"""
        payload = {}
        for job_id in self.jobs_data:
            payload[job_id] = {}
            job_data = self.jobs_data[job_id]
            payload_data = payload[job_id]
            payload_data["status"] = job_data.status
            payload_data["count"] = job_data.count
            payload_data["type"] = job_data.type
            payload_data["processed_files"] = job_data.processed_files
            payload_data["batch_size"] = job_data.batch_size
            payload_data["file_name"] = job_data.file_name
            payload_data["dispatch_files"] = job_data.dispatch_files
            payload_data["dispatch_timestamps"] = job_data.dispatch_timestamps
            payload_data["assigned_nodes"] = list(job_data.assigned_nodes)
            payload_data["start"] = job_data.start
            payload_data["end"] = job_data.end
            payload_data["files"] = list(job_data.files)
        return payload

    def convert_list_to_query_data(self):
        """convert list to query data"""
        try:
            for temp_id in self.jobs_data_back:
                job_id = int(temp_id)
                job_data = self.jobs_data_back[temp_id]
                self.jobs_data[job_id] = query_data_class(
                    job_id,
                    job_data["type"],
                    int(job_data["batch_size"]),
                    job_data["file_name"],
                )
                self.jobs_data[job_id].status = job_data["status"]
                self.jobs_data[job_id].count = int(job_data["count"])
                self.jobs_data[job_id].processed_files = job_data["processed_files"]
                self.jobs_data[job_id].dispatch_files = defaultdict(
                    list, job_data["dispatch_files"]
                )
                self.jobs_data[job_id].dispatch_timestamps = defaultdict(
                    list, job_data["dispatch_timestamps"]
                )
                self.jobs_data[job_id].assigned_nodes = deque(
                    job_data["assigned_nodes"]
                )
                self.jobs_data[job_id].start = float(job_data["start"])
                self.jobs_data[job_id].end = (
                    float(job_data["end"]) if job_data["end"] != None else None
                )
                self.jobs_data[job_id].files = deque(job_data["files"])
        except Exception as e:
            print("Error in converting list to query data", e)

    def exchange_job_info(self):
        """exchange job info with other ml_cord servers"""
        try:
            if HOSTNAME == "fa22-cs425-1902":
                return
            tcp_conn_obj = tcp_connection("fa22-cs425-1902", ML_CORD_PORT)
            tcp_conn_obj.connect_to_target()
            payload = message(
                QUERY_DATA, adjust_dict_cord(self.convert_query_data_to_list())
            )
            payload = payload.get_encoded_message()
            tcp_conn_obj.conn.sendall(payload)
            tcp_conn_obj.close_conn()
        except Exception as e:
            # print("Error in exchange_job_info", e)
            pass

    def check_and_restart_jobs(self):
        """check and restart jobs"""
        while True:
            sleep(2)
            try:
                if get_ml_cord(self.membership_list + [self.self_proc_id]) == HOSTNAME:
                    self.balance_jobs()
                    if HOSTNAME == "fa22-cs425-1901":
                        self.exchange_job_info()
                        continue
                if (
                    HOSTNAME == "fa22-cs425-1902"
                    and get_ml_cord(self.membership_list + [self.self_proc_id])
                    != HOSTNAME
                ):
                    continue
                if self.back_start == True:
                    continue
                self.back_start = True
                print("I'm the new ML CORD")
                print(self.jobs_data_back)
                self.convert_list_to_query_data()
                for temp_id in self.jobs_data:
                    job_id = int(temp_id)
                    query_data = self.jobs_data[temp_id]
                    if query_data.status == "COMPLETED":
                        continue
                    job_handler_obj = job_handler(
                        query_data,
                        self.membership_list,
                        self.self_proc_id,
                        self.metadata,
                        self.job_id,
                        self.base_port + self.job_id,
                        is_restart=True,
                    )
                    self.get_assigned_nodes()
                    thread = threading.Thread(target=job_handler_obj.handle_job)
                    thread.start()
                    thread = threading.Thread(target=job_handler_obj.serve)
                    thread.start()
                    self.base_port += 1
            except Exception as e:
                print("Error in check_and_restart_jobs", e)

    def get_all_jobs_info(self):
        """get all jobs info"""
        ret = {}
        for job in self.jobs_data:
            ret[job] = {}
            ret[job]["job_id"] = job
            ret[job]["type"] = self.jobs_data[job].type
            ret[job]["dataset"] = self.jobs_data[job].file_name
            ret[job]["batch_size"] = self.jobs_data[job].batch_size
            ret[job]["status"] = self.jobs_data[job].status
        return adjust_dict(ret)

    def get_last_ten_sec_stats(self, job_id):
        count = 0
        now = time.time()
        for timestamp in list(self.jobs_data[job_id].time_stamps[::-1]):
            if now - timestamp < 10:
                count += 1
            else:
                break
        return count / 10

    def get_total_stats(self, job_id):
        job_id = int(job_id)
        time_periods = []
        time_now = time.time()
        for i in self.jobs_data[job_id].dispatch_timestamps:
            if len(self.jobs_data[job_id].dispatch_timestamps[i]) == 2:
                time_periods.append(
                    self.jobs_data[job_id].dispatch_timestamps[i][1]
                    - self.jobs_data[job_id].dispatch_timestamps[i][0]
                )
        try:
            count = self.jobs_data[job_id].count
            mean = np.mean(time_periods)
            std = np.std(time_periods)
            seventhy_five = np.percentile(time_periods, 75)
            ninetynine = np.percentile(time_periods, 99)
            twentyfive = np.percentile(time_periods, 25)
            ten_sec_stats = self.get_last_ten_sec_stats(job_id)
            if self.jobs_data[job_id].status != "COMPLETED":
                tot_rate = self.jobs_data[job_id].count / (
                    time_now - self.jobs_data[job_id].start
                )
            else:
                tot_rate = self.jobs_data[job_id].count / (
                    self.jobs_data[job_id].end - self.jobs_data[job_id].start
                )
        except Exception as e:
            count = 0
            mean = 0
            std = 0
            seventhy_five = 0
            ninetynine = 0
            twentyfive = 0
            ten_sec_stats = 0
            tot_rate = 0
        return {
            "count": count,
            "mean": mean,
            "std": std,
            "75th": seventhy_five,
            "99th": ninetynine,
            "25th": twentyfive,
            "10_sec_rate": ten_sec_stats,
            "total_rate": tot_rate,
            "assigned_nodes": list(self.jobs_data[job_id].assigned_nodes),
        }

    def get_all_jobs_stats(self):
        ret = {}
        for job_id in self.jobs_data:
            job_id = int(job_id)
            time_periods = []
            time_now = time.time()
            for i in self.jobs_data[job_id].dispatch_timestamps:
                if len(self.jobs_data[job_id].dispatch_timestamps[i]) == 2:
                    time_periods.append(
                        self.jobs_data[job_id].dispatch_timestamps[i][1]
                        - self.jobs_data[job_id].dispatch_timestamps[i][0]
                    )
            try:
                count = self.jobs_data[job_id].count
                mean = np.mean(time_periods)
                std = np.std(time_periods)
                seventhy_five = np.percentile(time_periods, 75)
                ninetynine = np.percentile(time_periods, 99)
                twentyfive = np.percentile(time_periods, 25)
                ten_sec_stats = self.get_last_ten_sec_stats(job_id)
                if self.jobs_data[job_id].status != "COMPLETED":
                    tot_rate = self.jobs_data[job_id].count / (
                        time_now - self.jobs_data[job_id].start
                    )
                else:
                    tot_rate = self.jobs_data[job_id].count / (
                        self.jobs_data[job_id].end - self.jobs_data[job_id].start
                    )
            except Exception as e:
                count = 0
                mean = 0
                std = 0
                seventhy_five = 0
                ninetynine = 0
                twentyfive = 0
                ten_sec_stats = 0
                tot_rate = 0
            ret[job_id] = {
                "count": count,
                "mean": mean,
                "std": std,
                "75th": seventhy_five,
                "99th": ninetynine,
                "25th": twentyfive,
                "10_sec_rate": ten_sec_stats,
                "total_rate": tot_rate,
                "assigned_nodes": list(self.jobs_data[job_id].assigned_nodes),
            }
        return adjust_dict(ret)
