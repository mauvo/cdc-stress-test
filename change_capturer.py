from neo4j import GraphDatabase
from threading import Thread
import time
import sys
from multiprocessing import Process

retrieve_changes_cypher = """
CALL db.cdc.query($previous_id)
YIELD id, txId, seq, metadata, event
RETURN id, txId, seq, metadata, event
{}
"""

#id AS id2, txId AS txId2, seq AS seq2, metadata AS metadata2, event AS event2

get_current_id_cypher = """
CALL db.cdc.current() YIELD id RETURN id
"""

class cdc_threaded:

    def __init__(self, neo4j_info, limit=0):
        self.neo4j_info = neo4j_info
        self.driver = GraphDatabase.driver(neo4j_info.uri, auth=neo4j_info.auth)
        self.cypher = self.get_cypher(limit)
        self.reset()
    
    def reset(self):
        self.thread = None
        self.running = False
        self.previous_id = None
        self.first_event_size = None
        self.total_runtime = 0
    
    def get_cypher(self, limit=0):
        if limit <= 0:
            return retrieve_changes_cypher.format("")
        return retrieve_changes_cypher.format("LIMIT {}".format(limit))

    def start(self, capture_duration, start_delay=0):
        if self.running: 
            raise Exception("CDC Threaded already running")

        self.running = True
        self.start_time = time.time()
        self.total_captured = 0
        self.thread = Thread(target=self.cdc_thread, args=(self.driver, capture_duration, start_delay))
        self.thread.start()

    def wait_for_results(self):
        if self.thread:
            self.thread.join()
        total_captured = self.total_captured
        total_runtime = self.total_runtime
        first_event_size = self.first_event_size
        self.reset()
        return total_captured, total_runtime, first_event_size

    def cdc_thread(self, driver, capture_duration, start_delay):
        start_time = time.time()
        with driver.session(database="neo4j") as session:
            self.previous_id = self.reset_id_to_current(session)
            time.sleep(start_delay)
            while time.time() - start_time <= capture_duration + start_delay:
                self.total_captured += self.retrieve_changes(session)
            self.total_runtime = time.time() - start_time - start_delay
        
    def reset_id_to_current(self, session):
        result = session.run(get_current_id_cypher)
        return result.single()[0]

    def retrieve_changes(self, session):
        if self.previous_id == None:
            self.previous_id = self.reset_id_to_current(session)
        result = session.run(self.cypher, previous_id=self.previous_id)
        changes = []
        count = 0
        for record in result:
            count += 1
            self.previous_id = record.values()[0]
            if self.first_event_size == None:
                self.first_event_size = sys.getsizeof(str(record))
        return count