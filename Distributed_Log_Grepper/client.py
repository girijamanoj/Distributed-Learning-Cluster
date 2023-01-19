from constants import *
import socket
import threading
from datetime import datetime
import time
import argparse
from prettytable import PrettyTable
import json
from collections import defaultdict
from py_grepper import py_grepper
from functools import lru_cache


class Client:
    def __init__(self, host, args, counts, timemap, log_files):
        """Initialize client object with the following parameters

        Args:
            host ([str]): [Host Name. eg: fa22-cs425-1902]
            args ([dict]): [args store CLI arguments]
            counts ([dict]): [dictonary to track grep count received from server]
            timemap ([dict]): [dictonary that stores time taken for connections and queries]
            log_files ([dict]): [dictonary to store the log file name in reqpective thread]
        """
        self.port = PORT
        self.host = host
        self.args = args
        self.log_count = None
        self.counts = counts
        self.timemap = timemap
        log_files[host] = "vm" + str(int(host[-2:])) + ".log"

    def connect_to_node(self):
        """Opens socket and connects to server"""
        try:
            temp_conn = socket.socket()
            temp_conn.connect((self.host, self.port))
            self.conn = temp_conn
        except Exception as e:
            print("{}:{} is down!, {}".format(self.host, self.port, str(e)))

    def close_conn(self):
        """Closes connection"""
        try:
            self.conn.close()
        except:
            pass

    def recv_answer(self):
        """Receives query results"""
        try:
            log_data = self.conn.recv(BUFFER_SIZE)
        except:
            log_data = None
        if log_data is None or not log_data:
            return
        self.log_count = log_data
        self.counts[self.host] = int(self.log_count)

    def send_query(self):
        """Constructs the query, sends it to the server and fetch the results"""
        start = datetime.now()
        try:
            temp = {
                "pattern": self.args.get("pattern", None),
                "text": self.args.get("text", None),
                "cached": self.args.get("cached", 1),
            }
            temp = json.dumps(temp, indent=4).encode("UTF-8")
            try:
                self.conn.sendall(temp)
            except:
                pass
            self.recv_answer()
        except:
            print("Failed to send the query to host: {}".format(self.host))
            print("\nTrying to reconnect")
            try:
                temp_conn = self.connect_to_node(self.host)
                print("Connection established to host {}".format(self.host))
                try:
                    print("Trying to resend the query")
                    temp = {
                        "pattern": self.args.get("pattern"),
                        "text": self.args.get("text"),
                    }
                    temp = json.dumps(temp, indent=4).encode("UTF-8")
                    self.conn.sendall(temp)
                    self.recv_answer()
                except:
                    print("Retry failed!, ignoring the host {}".format(self.host))
            except Exception as e:
                print("Socket {} is down\n{}".format(self.host, self.port, str(e)))
        end = datetime.now()
        self.timemap[self.host] = [(end - start).total_seconds()]
        self.close_conn()

    def create_conn_and_query(self):
        """Parent function that triggers connect_to_node() and send_query()"""
        start = datetime.now()
        self.connect_to_node()
        self.send_query()
        end = datetime.now()
        self.timemap[self.host].append((end - start).total_seconds())


def api(pattern=None, text=None, cached=1):
    """api function can be used by other tools to run queries

    Args:
        pattern ([str], optional): [Regex pattern to match]. Defaults to None.
        text ([ste], optional): [Text to match]. Defaults to None.
        cached (int, optional): [0 to get response without cache, 1 to get cached response]. Defaults to 1.

    Returns:
        [tuple]: [(
                count: dictonary of responses of grep query mapped to respective hosts,
                log_files: dictonary that contains log file names of respective hosts,
                timemap: dictonary that contains latency information of respective hosts,
                total_count: int that contains aggregated count of grep query,
                total_time_cq: int that tracks the total time taken for connection and grep query,
                total_time_q: int that tracks the total time taken by the grep query,
                )]
    """
    if pattern is None and text is None:
        return None
    args = {"pattern": pattern, "text": text, "cached": cached}
    threads = []  # stores threads that contain Client object for each server
    counts = defaultdict(lambda: None)
    timemap = defaultdict(list)
    log_files = defaultdict(lambda: None)
    for node in all_nodes:
        temp = Client(node, args, counts, timemap, log_files)
        threads.append(threading.Thread(target=temp.create_conn_and_query))

    # Starts the threads
    for thread in threads:
        thread.start()

    # Waits for all the threads to complete their execution
    for thread in threads:
        thread.join()

    total_count = 0
    total_time_cq = 0
    total_time_q = 0
    for host in all_nodes:
        if counts[host] == None:
            total_time_cq += timemap[host][1]
            total_time_q += timemap[host][0]
            timemap[host] = ["Connection down!", "Connection down!"]
            counts[host] = "Connection down!"
            continue
        else:
            total_count += counts[host]
        total_time_cq += timemap[host][1]
        total_time_q += timemap[host][0]
    return (counts, log_files, timemap, total_count, total_time_cq, total_time_q)


def main():
    """main function is invoked when the process is triggered from CLI"""

    # parser will parse the CLI arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("--pattern", "-p", help="Query pattern")
    parser.add_argument("--text", "-t", help="Query text")
    parser.add_argument(
        "--cached",
        "-c",
        help="Cached grep: 0 for no_cache and 1 for cached",
        default=1,
        type=int,
    )

    args = parser.parse_args()
    if not args.pattern and not args.text:
        parser.print_help()
        exit(1)  # if not arguments are give, the process will exit

    threads = []  # stores threads that contain Client object for each server
    counts = defaultdict(lambda: None)
    timemap = defaultdict(list)
    log_files = defaultdict(lambda: None)
    args = {"pattern": args.pattern, "text": args.text, "cached": args.cached}
    for node in all_nodes:
        temp = Client(node, args, counts, timemap, log_files)
        threads.append(threading.Thread(target=temp.create_conn_and_query))

    # Starts the threads
    for thread in threads:
        thread.start()

    # Waits for all the threads to complete their execution
    for thread in threads:
        thread.join()

    # Printing output
    total_count = 0
    total_time_cq = 0
    total_time_q = 0
    table = PrettyTable()
    table.field_names = [
        "HOST",
        "LOG FILE",
        "LINES MATCHED",
        "TIME TAKEN FOR CONNECTION AND QUERY",
        "TIME TAKEN FOR QUERY",
    ]

    for host in all_nodes:
        if counts[host] == None:
            table.add_row(
                [host, "NA", "Connection down!", "Connection down!", "Connection down!"]
            )
        else:
            table.add_row(
                [
                    host,
                    log_files[host],
                    counts[host],
                    timemap[host][1],
                    timemap[host][0],
                ]
            )
            total_count += counts[host]
        total_time_cq += timemap[host][1]
        total_time_q += timemap[host][0]
    print(table)

    total_table = PrettyTable()
    total_table.field_names = [
        "TOTAL COUNT",
        "TOTAL TIME TAKEN FOR CONNECTIONS AND QUERIES",
        "TOTAL TIME TAKEN FOR QUERIES",
    ]
    total_table.add_row([total_count, total_time_cq, total_time_q])
    print(total_table)


if __name__ == "__main__":
    main()
