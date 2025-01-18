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

    def __init__(self, neo4j_info):
        self.neo4j_info = neo4j_info
        self.max_threads = multiprocessing.cpu_count() * 2
        if self.max_threads > 1:
            self.max_threads -= 1
        self.max_batch_size = 3_000
        self.lock = threading.Lock()
        self.reset()
    
    def reset(self):
        self.running = False
        self.session = None
        self.driver = None
        self.driver = GraphDatabase.driver(self.neo4j_info.uri, auth=self.neo4j_info.auth)
        self.running = False
        self.threads = None
        self.total_runtime = 0
        self.total_changes = 0
    
    def wait_for_results(self):
        if self.threads:
            for thread in self.threads:
                thread.join()
        total_changes = self.total_changes
        avg_runtime = self.total_runtime / self.max_threads
        self.reset()
        return total_changes, avg_runtime

    def start(self, duration, target_change_rate, payload_bytes=1):
        if self.running: 
            raise Exception("Change maker already running")

        self.running = True
        target_change_rate_per_thread = target_change_rate / self.max_threads
        self.threads = []
        for thread_id in range(self.max_threads):
            thread = Thread(target=self.change_maker_thread, args=(self.driver, thread_id, duration, target_change_rate_per_thread, payload_bytes))
            self.threads.append(thread)
        for thread in self.threads:
            thread.start()

    def change_maker_thread(self, driver, thread_id, duration, target_change_rate_per_thread, payload_bytes):
        my_start_time = time.time()
        my_total_changes = 0
        my_batch_size = self.max_batch_size
        if target_change_rate_per_thread > 0:
            my_batch_size = math.ceil(min(target_change_rate_per_thread/2, my_batch_size))

        with driver.session(database="neo4j") as session:
            while True:

                if time.time() - my_start_time > duration:
                    break

                create_nodes(session, my_batch_size, thread_id, payload_bytes=payload_bytes)
                my_total_changes += my_batch_size
                with self.lock:
                    self.total_changes += my_batch_size
                self.wait_if_necessary(my_start_time, my_total_changes, target_change_rate_per_thread, thread_id)

                if time.time() - my_start_time > duration:
                    break

                delete_nodes(session, thread_id)
                my_total_changes += my_batch_size
                with self.lock:
                    self.total_changes += my_batch_size
                self.wait_if_necessary(my_start_time, my_total_changes, target_change_rate_per_thread, thread_id)

        my_duration = time.time() - my_start_time
        with self.lock:
            self.total_runtime += my_duration

    def wait_if_necessary(self, my_start_time, my_total_changes, target_change_rate_per_thread, thread_id):
        if target_change_rate_per_thread <= 0:
            return

        elapsed_time = time.time() - my_start_time
        expected_changes = elapsed_time * target_change_rate_per_thread

        excess_changes = my_total_changes - expected_changes
        if excess_changes <= 0:
            return

        required_delay = excess_changes / target_change_rate_per_thread
        time.sleep(required_delay)