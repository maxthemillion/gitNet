from py2neo import Graph


class Neo4jController:
    def __init__(self):
        self.graph = Graph(user="max", password="1111")

    def run_louvain(self):
        print("Running Louvain algorithm on Neo4j...")
        query_part = "CALL algo.louvain(" \
                     "'MATCH (u:USER) RETURN id(p) as id', " \
                     "'MATCH (u1:USER)-[rel:REFERS_TO]-(u2:USER) " \
                     "RETURN id(u1) as source, id(u2) as target', " \
                     "{weightProperty:'weight', write: true, writeProperty:'community', graph:'cypher'})"
        self.graph.run(query_part)
        print("Complete!")
        print()

    def stream_to_gephi(self):
        print("Streaming network to Gephi...")
        query_part = "MATCH path = (:USER)-[:REFERS_TO]-(:USER)" \
                     "CALL apoc.gephi.add(null, 'workspace1', path, 'weight', ['community']) " \
                     "YIELD nodes " \
                     "return *"
        self.graph.run(query_part)
        print("Complete!")
        print()
