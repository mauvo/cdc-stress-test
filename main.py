from neo4j_utils import Neo4jInfo
import tests
import time
import change_capturer
from change_maker import changer

NEO4J_USERNAME = "neo4j"
NEO4J_URI_5 = "neo4j+s://e99a07ba.databases.neo4j.io"
NEO4J_PASSWORD_5 = "nJZkOnHsuix3yLicZJCRZ0GHEhZEXmd-Hr4usLUhZuI"
NEO4J_AUTH_5 = (NEO4J_USERNAME, NEO4J_PASSWORD_5)
NEO4j_DATABASE = "neo4j"

def performance_test(cdc, cm, neo4j_info, target_change_rate, test_time, payload_bytes=1):
  neo4j_info.delete_all_nodes()
  time.sleep(1)
  cdc.open(neo4j_info)
  cm.open(neo4j_info, target_change_rate=target_change_rate, payload_bytes=payload_bytes)
  cdc.start()
  cm.start()
  time.sleep(test_time)
  cm.send_stop()
  cdc.send_stop()
  time.sleep(0.001)
  total_captured, total_cdc_time, first_event_size = cdc.wait_stop()
  total_changes, total_cm_time = cm.wait_stop()
  cm.close()
  cdc.close()
  return total_changes, total_cm_time, total_captured, total_cdc_time, first_event_size

def profile(cdc, cm, neo4j_info, initial_rate, rate_increment, final_rate):
    rate = initial_rate
    test_time = 10
    while rate <= final_rate:
        total_changes, total_cm_time, total_captured, total_cdc_time, first_event_size = performance_test(cdc, cm, neo4j_info, rate, test_time)
        print("{:,.0f}\t{:,.0f}\t{:,.0f}\t{:,.0f}\t{:,.1f}\t{:,.0f}\t{:,.1f}".format(first_event_size, rate, test_time, total_changes, total_cm_time, total_captured, total_cdc_time))
        rate += rate_increment
   

if __name__ == '__main__':
    neo4j_info = Neo4jInfo(NEO4J_URI_5, NEO4J_AUTH_5, 5, NEO4j_DATABASE)
    cm = changer()
    cdc = change_capturer.cdc_threaded()
    #tests.run_all_tests(neo4j_info)
    tests.test_performance_test(neo4j_info)
    #profile(cdc, cm, neo4j_info, 250, 250, 5000)