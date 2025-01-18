from neo4j import GraphDatabase

class Neo4jInfo():
  
    def __init__(self, uri, auth, version, database):
        self.uri = uri
        self.auth = auth
        self.version = version
        self.database = database

    def can_connect(self):
        with GraphDatabase.driver(self.uri, auth=self.auth) as driver:
            try:
                driver.verify_connectivity() #Throws if no connection
                return True
            except Exception as e:
                print(e)
                return False
            
    def run_cypher(self, cypher, **kwargs):
        with GraphDatabase.driver(self.uri, auth=self.auth) as driver:
            with driver.session(database="neo4j") as session:
                return run_cypher(session, cypher, kwargs)
    
    def count_nodes(self, label_id=""):
        with GraphDatabase.driver(self.uri, auth=self.auth) as driver:
            with driver.session(database=self.database) as session:
                return count_nodes(session, label_id)

    def delete_all_nodes(neo4j_info, label_id=""):
        with GraphDatabase.driver(neo4j_info.uri, auth=neo4j_info.auth) as driver:
            with driver.session(database="neo4j") as session:
                return delete_all_nodes(session, neo4j_info, label_id)

def run_cypher(session, cypher, **kwargs):
    result = session.run(cypher, kwargs)
    values = []
    for record in result:
        values.append(record.values())
    summary = result.consume()
    return summary.counters

#Count and delete nodes, to reset database when required

count_nodes_cypher = """
MATCH (n{})
RETURN count(n)
"""

delete_all_cypher_5 = """
MATCH (n{})
CALL (n) {{
  DETACH DELETE n
}}
IN TRANSACTIONS OF 1000 ROWS
"""

def get_count_nodes_cypher(label_id = ""):
    if label_id != "":
        return count_nodes_cypher.format(":label_id_{}".format(label_id))
    else:
        return count_nodes_cypher.format("")

def get_delete_nodes_cypher(label_id = ""):
  if label_id != "":
    return delete_all_cypher_5.format(":label_id_{}".format(label_id))
  else:
    return delete_all_cypher_5.format("")

def count_nodes(session, label_id=""):
    cypher = get_count_nodes_cypher(label_id)
    result = session.run(cypher)
    count = 0
    for record in result:
        count += record.values()[0]
    summary = result.consume()
    return count

def delete_all_nodes(session, neo4j_info, label_id=""):
    cypher = get_delete_nodes_cypher(label_id)
    result = session.run(cypher)
    count = 0
    for record in result:
        count += record.values()[0]
    summary = result.consume()
    return count