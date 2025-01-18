from neo4j import GraphDatabase
import neo4j_utils
import change_maker
import change_capturer
import time
import main

def run_all_tests(neo4j_info):
    print("Basic tests")
    if neo4j_info.can_connect():
        print("\tCan connect to v5")
    else:
        print("\tFailed to connect to v5")
        return
    print("\tCounted {} nodes on v{}".format(neo4j_info.count_nodes(), neo4j_info.version))
    print("\tDeleted {} nodes on v{}".format(neo4j_info.delete_all_nodes(), neo4j_info.version))
    print("\tCounted {} nodes on v{}".format(neo4j_info.count_nodes(), neo4j_info.version))
    print("Create/delete tests")
    label_id = "test"
    print("\tCounted {} nodes with label {} on v{}".format(neo4j_info.count_nodes(label_id), label_id, neo4j_info.version))
    test_create_nodes(neo4j_info, 100, label_id, 800)
    print("\tCounted {} nodes with label {} on v{}".format(neo4j_info.count_nodes(label_id), label_id, neo4j_info.version))
    test_delete_nodes(neo4j_info, label_id)
    print("\tCounted {} nodes with label {} on v{}".format(neo4j_info.count_nodes(label_id), label_id, neo4j_info.version))
    print("Test change capturer")
    test_cdc(neo4j_info)
    print("Test change capturer threading")
    test_cdc_threading(neo4j_info)
    print("Test changer")
    target_change_rate = 5_000
    total_changes, total_runtime = test_make_changes(neo4j_info, target_change_rate, 10)
    print("\tTarget change rate: {:,.0f}".format(target_change_rate))
    print("\tActual change rate: {:,.0f}".format(total_changes/total_runtime))
    print("\tTotal time: {}".format(total_runtime))
    print("\tTotal changes: {}".format(total_changes))
    print("Test changer on target rates")
    test_changer_can_hit_targets(neo4j_info)
    print("Test performance test")
    test_performance_test(neo4j_info)

def test_create_nodes(neo4j_info, node_count, label_id, payload_bytes):
    with GraphDatabase.driver(neo4j_info.uri, auth=neo4j_info.auth) as driver:
        with driver.session(database=neo4j_info.database) as session:
            change_maker.create_nodes(session, node_count, label_id, payload_bytes)

def test_delete_nodes(neo4j_info, label_id):
    with GraphDatabase.driver(neo4j_info.uri, auth=neo4j_info.auth) as driver:
        with driver.session(database=neo4j_info.database) as session:
            change_maker.delete_nodes(session, label_id)

def test_cdc(neo4j_info):
    cdc = change_capturer.cdc_threaded()

    label_id = "test_1"
    test_delete_nodes(neo4j_info, label_id)
    print("\tCounted {} nodes with label {} on v{}".format(neo4j_info.count_nodes(label_id), label_id, neo4j_info.version))

    cdc.open(neo4j_info)
    cdc.start()
    test_create_nodes(neo4j_info, 100, label_id, 1)
    print("\tCounted {} nodes with label {} on v{}".format(neo4j_info.count_nodes(label_id), label_id, neo4j_info.version))
    count = cdc.total_captured
    print("\tRetrieved {} event changes".format(count))
    print("\tDeleting Nodes...")
    test_delete_nodes(neo4j_info, label_id)
    print("\tCounted {} nodes with label {} on v{}".format(neo4j_info.count_nodes(label_id), label_id, neo4j_info.version))
    time.sleep(1)
    count = cdc.total_captured
    print("\tRetrieved {} event changes".format(count))
    total_captured, total_time, first_event_size = cdc.wait_stop()
    cdc.close()
    print("\tTotal time: {}".format(total_time))
    print("\tTotal captured: {}".format(total_captured))
    print("\tFirst event size: {}".format(first_event_size))

#Single-threaded stress test of CDC
def test_cdc_threading(neo4j_info):
    cdc = change_capturer.cdc_threaded()
    changed_count = 0
    label_id = "test_3"
    cdc.open(neo4j_info)
    with GraphDatabase.driver(neo4j_info.uri, auth=neo4j_info.auth) as driver:
        with driver.session(database=neo4j_info.database) as session:
            start = time.time()
            cdc.start()
            while(time.time() - start < 10):
                change_maker.create_nodes(session, 50, label_id, 1)
                changed_count += 50
                change_maker.delete_nodes(session, label_id)
                changed_count += 50
                time.sleep(1)
            cdc.send_stop()

    total_captured, total_time, first_event_size = cdc.wait_stop()
    cdc.close()

    print("\tChange count {}, Captured count {}".format(changed_count, total_captured))

def test_make_changes(neo4j_info, target_change_rate, test_duration):
  cm = change_maker.changer()
  cm.open(neo4j_info, target_change_rate=target_change_rate)
  cm.start()
  time.sleep(test_duration)
  total_changes, total_runtime = cm.wait_stop()
  cm.close()
  return total_changes, total_runtime

def test_changer_can_hit_targets(neo4j_info):
    target_rate = 1000
    increment = 1000
    test_duration = 10

    while target_rate <= 5_000:
        total_changes, total_runtime = test_make_changes(neo4j_info, target_rate, test_duration)
        print("\tTarget rate: {:,.0f}, Actual change rate: {:,.0f}, Target time: {:,.1f}, Actual time: {:,.1f}".format(target_rate, total_changes/total_runtime, test_duration, total_runtime))
        target_rate += increment

def test_performance_test(neo4j_info):
    cm = change_maker.changer()
    cdc = change_capturer.cdc_threaded()
    target_change_rate = 3000
    test_time = 10 #seconds
    payload_bytes = 1
    total_changes, total_cm_time, total_captured, total_cdc_time, first_event_size = main.performance_test(cdc, cm, neo4j_info, target_change_rate, test_time, payload_bytes)

    print("\tTarget Rate: {:,.0f}, Test Time: {:,.0f}".format(target_change_rate, test_time))
    print("\tTotal Changes: {:,.0f}, Total Time: {:,.1f}s, Change Rate: {:,.0f}".format(total_changes, total_cm_time, total_changes/total_cm_time))
    print("\tTotal Events: {:,.0f}, Total Time: {:,.1f}s, Event Rate: {:,.0f}".format(total_captured, total_cdc_time, total_captured/total_cdc_time))
    print("\tFirst event size: {:,.0f} bytes".format(first_event_size))