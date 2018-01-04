from py2neo import Graph
import json
import pandas as pd


class Neo4jController:
    def __init__(self):
        self.graph = Graph(user="max", password="1111")
        # TODO: collect paths as object attributes

    def clear_db(self):
        query = "MATCH (n) DETACH DELETE n"
        self.graph.run(query)

    def import_graph(self):
        # TODO: check if it makes sense to transfer the nodes directly to Neo4j
        # csv export-import could be replaced by something faster.

        # import the CSV files to Neo4j using Cypher
        import_path = \
            "'file:///Users/Max/Desktop/MA/Python/projects/NetworkConstructor/Export/participants_export.csv'"

        # create user nodes if these do not already exist
        # 'MERGE' matches new patterns to existing ones. If it doesn't exist, it creates a new one
        query_create_users = "LOAD CSV WITH HEADERS FROM " + import_path + \
                             "AS row FIELDTERMINATOR ';'" \
                             "MERGE (:USER{login:row.participants})"
        self.graph.run(query_create_users)

        import_path = \
            "'file:///Users/Max/Desktop/MA/Python/projects/NetworkConstructor/Export/references_export.csv'"

        query_create_ref = '''LOAD CSV WITH HEADERS FROM ''' + import_path + \
                           '''AS row FIELDTERMINATOR ';'
                           MERGE (a:USER{login:row.commenter})
                           MERGE (b:USER{login:row.addressee})
                           WITH a, b, row
                           CALL apoc.create.relationship(a, row.ref_type, {weight: row.weight, timestamp:row.timestamp}, b)
                           YIELD rel
                           RETURN rel'''

        self.graph.run(query_create_ref)

        print("Export to Neo4j succeeded!")
        print()

    def get_communities(self):

        cut_val = 10
        query = "MATCH (n:USER) " \
                "WITH COUNT(n.community) AS count, n.community as community  " \
                "WHERE count > 1 " \
                "RETURN count, community " \
                "ORDER BY count DESC"

        communities = pd.DataFrame(self.graph.data(query))
        communities = communities[communities["count"] > cut_val]

        return communities

    def get_high_degree_nodes(self):
        # TODO: Check this query! Does it deliver what you expect?
        # From each group, get the node with the highest degree.
        # cut_val = 10
        query = "MATCH r = (n:USER)-[x]-() " \
                "WITH Count(x) as node_degree, n " \
                "ORDER BY n.community, node_degree DESC " \
                "WITH n.community AS group, Collect(n)[..1] as topNodes " \
                "UNWIND topNodes as n2 " \
                "MATCH (n2)-[x]-() " \
                "RETURN n2.login AS name, n2.community as group, Count(x) AS n_degree " \
                "ORDER BY n_degree DESC "

        nodes = pd.DataFrame(self.graph.data(query))
        # nodes = nodes[nodes["n_degree"] > cut_val]
        return nodes

    def get_degree(self):
        query = "MATCH r = (n:USER)-[x]-() " \
                "RETURN Count(x) as node_degree, n.login as name" \

        nodes = pd.DataFrame(self.graph.data(query))
        return nodes

    def run_louvain(self):
        print("Running Louvain algorithm on Neo4j...")
        query_part = "CALL algo.louvain(" \
                     "'MATCH (u:USER) RETURN id(p) as id', " \
                     "'MATCH (u1:USER)-[rel]-(u2:USER) " \
                     "RETURN id(u1) as source, id(u2) as target', " \
                     "{weightProperty:'weight', write: true, writeProperty:'community', graph:'cypher'})"
        self.graph.run(query_part)
        print("Complete!")
        print()

    def stream_to_gephi(self):
        print("Streaming network to Gephi...")
        query_part = "MATCH path = (:USER)--(:USER)" \
                     "CALL apoc.gephi.add(null, 'workspace1', path, 'weight', ['community']) " \
                     "YIELD nodes " \
                     "return *"
        self.graph.run(query_part)
        print("Complete!")
        print()

    def find_id(self, id, nodes):
        for index, node in enumerate(nodes):
            if node["node_id"] == id:
                return index

    def export_graphjson(self):
        print("exporting data in graphJSON format...")
        query1 = "MATCH (n:USER) " \
                 "RETURN n.login AS id, " \
                 "n.community AS group, " \
                 "id(n) AS node_id"

        query2 = "MATCH (a)-[r]->(b) " \
                 "RETURN a.login AS source, " \
                 "b.login AS target, " \
                 "r.weight AS weight, " \
                 "type(r) as rel_type, " \
                 "r.timestamp as timestamp, " \
                 "id(r) AS link_id"

        nodes = self.graph.data(query1)

        communities = self.get_communities()
        num_no_community = len(communities) + 1

        high_degree_nodes = self.get_high_degree_nodes()
        node_degree = self.get_degree()

        for node in nodes:
            if node["group"] in communities["community"].values:
                idx = communities[communities["community"] == node["group"]].index[0]
                node["group"] = int(idx)
                node["hasGroup"] = True
            else:
                node["group"] = num_no_community
                node["hasGroup"] = False

            if node["id"] in node_degree["name"].values:
                idx = node_degree[node_degree["name"] == node["id"]].index[0]
                node["degree_py"] = int(node_degree["node_degree"].iloc[idx])
            else:
                node["degree_py"] = 0

            if node["id"] in high_degree_nodes["name"].values:
                node["highestDegreeInGroup"] = True
            else:
                node["highestDegreeInGroup"] = False

        links = self.graph.data(query2)
        for link in links:
            link["weight"] = int(link["weight"])  # TODO: find the cause for weight being a string
            # link["source"] = self.find_id(link["source"], nodes)
            # link["target"] = self.find_id(link["target"], nodes)

        data = {"nodes": nodes, "links": links}

        with open("Export/data.json", "w") as fp:
            json.dump(data, fp, indent="\t")

