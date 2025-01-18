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

    def __init__(self, neo4j_info):
        self.neo4j_info = neo4j_info
        self.driver = GraphDatabase.driver(neo4j_info.uri, auth=neo4j_info.auth)
        self.reset()
    
    def reset(self):
        self.thread = None
        self.running = False
        self.previous_id = None
        self.first_event_size = None
        self.total_runtime = 0

    def start(self, capture_duration):
        if self.running: 
            raise Exception("CDC Threaded already running")

        self.running = True
        self.start_time = time.time()
        self.total_captured = 0
        self.thread = Thread(target=self.cdc_thread, args=(self.driver, capture_duration))
        self.thread.start()

    def wait_for_results(self):
        if self.thread:
            self.thread.join()
        total_captured = self.total_captured
        total_runtime = self.total_runtime
        first_event_size = self.first_event_size
        self.reset()
        return total_captured, total_runtime, first_event_size

    def cdc_thread(self, driver, capture_duration):
        start_time = time.time()
        with driver.session(database="neo4j") as session:
            while time.time() - start_time <= capture_duration:
                self.total_captured += self.retrieve_changes(session)
            self.total_runtime = time.time() - start_time
        
    def reset_id_to_current(self, session):
        result = session.run(get_current_id_cypher)
        return result.single()[0]

    def retrieve_changes(self, session):
        if self.previous_id == None:
            self.previous_id = self.reset_id_to_current(session)
        result = session.run(retrieve_changes_cypher, previous_id=self.previous_id)
        changes = []
        count = 0
        for record in result:
            count += 1
            self.previous_id = record.values()[0]
            if self.first_event_size == None:
                self.first_event_size = sys.getsizeof(str(record))
        return count