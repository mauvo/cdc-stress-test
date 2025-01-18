from neo4j import GraphDatabase
from threading import Thread
import time
import sys
from multiprocessing import Process

retrieve_changes_cypher = """
CALL db.cdc.query($previous_id)
YIELD id, txId, seq, metadata, event
RETURN id, txId, seq, metadata, event
LIMIT 10000
"""

get_current_id_cypher = """
CALL db.cdc.current() YIELD id RETURN id
"""

class cdc_threaded:

    def __init__(self):
        self.neo4j_info = None
        self.driver = None
        self.session = None
        self.previous_id = None
        self.running = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def open(self, neo4j_info):
        self.close()
        self.neo4j_info = neo4j_info
        self.driver = GraphDatabase.driver(neo4j_info.uri, auth=neo4j_info.auth)
        self.session = self.driver.session(database=neo4j_info.database)
        self.previous_id = None
        self.first_event_size = None

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
        self.total_captured = 0

        self.thread = Thread(target=self.cdc_thread, args=(self.driver, lambda : self.running))
        self.thread.start()

    def send_stop(self):
        self.running = False
        self.stop_thread = Thread(target=self.time_stop_thread, args=())
        self.stop_thread.start()

    def time_stop_thread(self):
        self.thread.join()
        self.stop_time = time.time()
        self.total_runtime = self.stop_time - self.start_time

    def wait_stop(self):
        self.send_stop()
        self.stop_thread.join()
        return self.total_captured, self.total_runtime, self.first_event_size

    def cdc_thread(self, driver, running):
        with driver.session(database="neo4j") as session:
            while running():
                self.total_captured += self.retrieve_changes(session)
                time.sleep(0.001)

    def reset_id_to_current(self):
        result = self.session.run(get_current_id_cypher)
        return result.single()[0]

    def retrieve_changes(self, session):
        if self.previous_id == None:
            self.previous_id = self.reset_id_to_current()
        result = session.run(retrieve_changes_cypher, previous_id=self.previous_id)
        changes = []
        count = 0
        for record in result:
            count += 1
            self.previous_id = record.values()[0]
            if self.first_event_size == None:
                self.first_event_size = sys.getsizeof(str(record))
        return count