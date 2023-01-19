from numpy import argmax, expand_dims
from tensorflow.keras.preprocessing.image import load_img
from tensorflow.keras.preprocessing.image import img_to_array
from tensorflow.keras.models import load_model
from mp2_constants import *
from message import message
from mp3_constants import *
from mp4_constants import *
from collections import deque
import time
from file_handler import file_handler
from utils import *
from tcp_connection import tcp_connection
import cv2
from file_handler import file_handler


class ml_server:
    """ML server that runs on each node"""

    def __init__(self, membership_list, self_proc_id, acker_obj) -> None:
        """Initialize the ML server

        Args:
            membership_list (list): List of all the nodes in the system
            self_proc_id (str): Process ID of the current node
            acker_obj (acker): Acker object
        """
        self.job_queue = deque([])
        self.message_decoder = message()
        self.self_proc_id = self_proc_id
        self.acker_obj = acker_obj
        self.membership_list = membership_list
        self.loaded = False

    def initialize_model(self):
        """downloads the model from sdfs and loads it"""
        if self.loaded:
            return
        local_file_name = "lenet_model.h5"
        sdfs_file_name = "lenet_model.h5"
        file_handler_obj = file_handler(
            self.membership_list,
            local_file_name=local_file_name,
            sdfs_file_name=sdfs_file_name,
            SELF_PROC_ID=self.self_proc_id,
            acker_obj=self.acker_obj,
        )
        done = file_handler_obj.download_file()
        local_file_name = "resnet_model.h5"
        sdfs_file_name = "resnet_model.h5"
        file_handler_obj = file_handler(
            self.membership_list,
            local_file_name=local_file_name,
            sdfs_file_name=sdfs_file_name,
            SELF_PROC_ID=self.self_proc_id,
            acker_obj=self.acker_obj,
        )
        self.lenet_model = load_model("downloads/lenet_model.h5")
        self.resnet_model = load_model("downloads/resnet_model.h5")
        self.loaded = True

    def serve(self):
        # self.lenet_model._make_predict_function()
        try:
            host = HOSTNAME
            port = ML_SERVER_PORT
            self.server = socket.socket()
            self.server.bind((host, port))
            # It will enable the server object to listen to 10 concurrent queries.
            self.server.listen(10)

        except Exception as e:
            print("Server failed\n{}".format(str(e)))
        self.recv()

    def recv(self):
        """Listens for incoming connections and download requests"""
        while True:
            try:
                self.conn, self.address = self.server.accept()
                self.address = self.address[0]
                recvd_message = self.conn.recv(BUFFER_SIZE)
                try:
                    self.conn.close()
                except Exception as e:
                    print("Error in closing ml_server connection: {}".format(e))
                if self.loaded == False:
                    self.initialize_model()
                recvd_message = self.message_decoder.decode_message(recvd_message)
                self.job_queue.append(
                    {
                        "job_id": recvd_message["D"]["job_id"],
                        "address": self.address,
                        "port": recvd_message["D"]["port"],
                        "type": recvd_message["D"]["type"],
                        "files": recvd_message["D"]["files"],
                    }
                )
            except Exception as e:
                print("Error in ml_server recv", e)

    def process_jobs(self):
        while True:
            if len(self.job_queue) == 0:
                continue
            else:
                job = self.job_queue.popleft()
                if job["type"] == "lenet":
                    self.process_lenet(job)
                if job["type"] == "resnet":
                    self.process_resnet(job)

    def download_file(self, file_name):
        file_handler_obj = file_handler(
            self.membership_list + [self.self_proc_id],
            file_name,
            file_name,
            self.self_proc_id,
            acker_obj=self.acker_obj,
        )
        file_handler_obj.download_file()

    def notify_ml_cord(self, files, addr, port, dispatch_timestamps):
        """Notifies the ML coordinator that the job is done"""
        payload = {"files": files, "dispatch_timestamps": dispatch_timestamps}
        payload = message(JOB_INFO, payload)
        payload = payload.get_encoded_message()
        cords = get_cords()
        for ML_CORD in cords:
            tcp_conn_obj = tcp_connection(ML_CORD, port)
            tcp_conn_obj.connect_to_target()
            tcp_conn_obj.conn.sendall(payload)
            tcp_conn_obj.conn.close()

    def upload_data(self, results):
        file_handler_obj = file_handler(
            self.membership_list,
            str(results["job_id"]) + ".csv",
            str(results["job_id"]) + ".csv",
            self.self_proc_id,
            acker_obj=self.acker_obj,
        )
        file_handler_obj.append_data(results)

    def process_lenet(self, job):
        """Processes the lenet job"""
        results = {"job_id": job["job_id"], "dispatch_timestamps": {}, "results": {}}
        for file_name in job["files"]:
            self.download_file(file_name)
            results["dispatch_timestamps"][file_name] = [time.time()]
        for file_name in job["files"]:
            img = load_img(
                "downloads/" + file_name, grayscale=True, target_size=(28, 28)
            )
            img = img_to_array(img)
            img = img.reshape(1, 28, 28, 1)
            img = img.astype("float32")
            img = img / 255.0
            digit = argmax(self.lenet_model.predict(img))
            results["results"][file_name] = str(digit)
        try:
            self.upload_data(results)
            for file_name in job["files"]:
                results["dispatch_timestamps"][file_name].append(time.time())
            notified = False
            while not notified:
                try:
                    self.notify_ml_cord(
                        job["files"],
                        job["address"],
                        job["port"],
                        results["dispatch_timestamps"],
                    )
                    notified = True
                except Exception as e:
                    print("Error in notifying ml_cord", e)
                    print("retrying in 5 seconds")
                    time.sleep(5)
        except Exception as e:
            print("Error in processing lenet job", e)

    def process_resnet(self, job):
        """Processes the resnet job"""
        classes = ["sunflowers", "daisy", "roses"]
        results = {"job_id": job["job_id"], "dispatch_timestamps": {}, "results": {}}
        for file_name in job["files"]:
            self.download_file(file_name)
            results["dispatch_timestamps"][file_name] = [time.time()]
        for file_name in job["files"]:
            img = cv2.imread("downloads/" + file_name)
            img = cv2.resize(img, (180, 180))
            img = expand_dims(img, axis=0)
            pred = self.resnet_model.predict(img)
            output_class = classes[argmax(pred)]
            results["results"][file_name] = str(output_class)
        try:
            self.upload_data(results)
            for file_name in job["files"]:
                results["dispatch_timestamps"][file_name].append(time.time())
            notified = False
            while not notified:
                try:
                    self.notify_ml_cord(
                        job["files"],
                        job["address"],
                        job["port"],
                        results["dispatch_timestamps"],
                    )
                    notified = True
                except Exception as e:
                    print("Error in notifying ml_cord", e)
                    print("retrying in 5 seconds")
                    time.sleep(5)
        except Exception as e:
            print("Error in processing resnet job", e)
