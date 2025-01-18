import neo4j_utils
import multiprocessing
import math
import time
from time import perf_counter
import threading
from threading import Thread
from neo4j import GraphDatabase

create_nodes_cypher = """
UNWIND range(1,$node_count) AS i
CREATE (n:label_id_{label})
SET n.payload = $payload
"""

delete_nodes_cypher = """
MATCH (n:label_id_{label})
DETACH DELETE n
"""

def get_create_nodes_cypher(label_id):
  return create_nodes_cypher.format(label=label_id)

def get_delete_nodes_cypher(label_id):
  return delete_nodes_cypher.format(label=label_id)

def get_payload(payload_bytes):
  return bytes(payload_bytes)

def create_nodes(session, node_count, label_id, payload_bytes):
  cypher = get_create_nodes_cypher(label_id)
  payload = get_payload(payload_bytes)
  neo4j_utils.run_cypher(session, cypher, node_count=node_count, payload=payload)

def delete_nodes(session, label_id):
  cypher = get_delete_nodes_cypher(label_id)
  neo4j_utils.run_cypher(session, cypher)

class changer:

    def __init__(self):
        self.max_threads = multiprocessing.cpu_count() * 2
        if self.max_threads > 1:
            self.max_threads -= 1
        self.max_batch_size = 3_000
        self.running = False
        self.session = None
        self.driver = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def open(self, neo4j_info, target_change_rate=1, payload_bytes=1):
        self.close()
        self.lock = threading.Lock()
        self.neo4j_info = neo4j_info
        self.driver = GraphDatabase.driver(neo4j_info.uri, auth=neo4j_info.auth)
        self.session = self.driver.session(database=neo4j_info.database)
        self.target_change_rate = target_change_rate
        self.target_change_rate_per_thread = -1
        if self.target_change_rate > 0:
            self.target_change_rate_per_thread = target_change_rate / self.max_threads
        self.payload_bytes = payload_bytes

    def close(self):
        if self.running: self.wait_stop()
        if self.session is not None:
            self.session.close()
            self.session = None
        if self.driver is not None:
            self.driver.close()
            self.driver = None

    def start(self):
        if self.running: return

        self.running = True
        self.start_time = time.time()
        self.total_changes = 0

        self.threads = []
        for thread_id in range(self.max_threads):
            thread = Thread(target=self.change_maker_thread, args=(self.driver, thread_id, self.target_change_rate_per_thread, lambda : self.running))
            self.threads.append(thread)
        for thread in self.threads:
            thread.start()

    def send_stop(self):
        self.stop_thread = Thread(target=self.time_stop_thread, args=())
        self.stop_thread.start()

    def time_stop_thread(self):
        self.running = False
        for thread in self.threads:
            thread.join()
        self.stop_time = time.time()
        self.total_runtime = self.stop_time - self.start_time

    def wait_stop(self):
        self.send_stop()
        self.stop_thread.join()
        return self.total_changes, self.total_runtime

    def change_maker_thread(self, driver, thread_id, target_change_rate_per_thread, running):
        my_start_time = perf_counter()
        my_total_changes = 0
        my_batch_size = self.max_batch_size
        if target_change_rate_per_thread > 0:
            my_batch_size = math.ceil(min(target_change_rate_per_thread/2, my_batch_size))

        with driver.session(database="neo4j") as session:
            while running():

                create_nodes(session, my_batch_size, thread_id, payload_bytes=self.payload_bytes)
                my_total_changes += my_batch_size
                with self.lock:
                    self.total_changes += my_batch_size
                self.wait_if_necessary(my_start_time, my_total_changes, target_change_rate_per_thread, thread_id)

                if not running(): break
                time.sleep(0.001)

                delete_nodes(session, thread_id)
                my_total_changes += my_batch_size
                with self.lock:
                    self.total_changes += my_batch_size
                self.wait_if_necessary(my_start_time, my_total_changes, target_change_rate_per_thread, thread_id)
                time.sleep(0.001)

    def wait_if_necessary(self, my_start_time, my_total_changes, target_change_rate_per_thread, thread_id):
        if target_change_rate_per_thread <= 0:
            return

        elapsed_time = perf_counter() - my_start_time
        expected_changes = elapsed_time * target_change_rate_per_thread

        excess_changes = my_total_changes - expected_changes
        if excess_changes <= 0:
            return

        required_delay = excess_changes / target_change_rate_per_thread
        time.sleep(required_delay)