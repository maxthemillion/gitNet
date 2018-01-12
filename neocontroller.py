from py2neo import Graph
import json
import pandas as pd
import warnings
import conf


class Neo4jController:
    def __init__(self):
        self.graph = Graph(user="max", password="1111")
        # TODO: collect paths as object attributes

    def clear_db(self):
        if conf.neo4j_clear_on_startup:
            print("clearing neo4j database")
            print()

            query = "MATCH (n) DETACH DELETE n"
            self.graph.run(query)

    def import_project(self, ref_df, node_df, owner, repo):
        if not conf.neo4j_import:
            return

        merge_ref = '''MERGE (a:USER{login:$l_login_a})
                    MERGE (b:USER{login:$l_login_b})
                    WITH a, b
                    CALL apoc.create.relationship(a, $l_ref_type, 
                            {weight: $l_weight, 
                            timestamp:$l_timestamp, 
                            owner: $l_owner,
                            repo: $l_repo
                            }, b)
                    YIELD rel
                    RETURN rel'''

        tx = self.graph.begin()
        for index, row in ref_df.iterrows():

            tx.evaluate(merge_ref, parameters={'l_login_a': row['commenter'],
                                               'l_login_b': row['addressee'],
                                               'l_ref_type': row['ref_type'],
                                               'l_weight': 1,
                                               'l_timestamp': row['timestamp'].strftime("%Y-%m-%dT%H:%M:%S%Z"),
                                               'l_owner': owner,
                                               'l_repo': repo})
            if (index + 1) % 10000 == 0:
                tx.commit()
                warnings.warn("batch commit to neo4j at " + index)
                tx = self.graph.begin()
        tx.commit()

        merge_nodes = '''MERGE (a:USER{login:$l_login_a})
                      MERGE (r:REPO{name:$l_repo})
                      MERGE (o:OWNER{name:$l_owner})
                      WITH a, r, o
                      MERGE (a)-[x:TOUCHED]->(r)
                      MERGE (r)-[y:BELONGS_TO]->(o)
                      RETURN x, y'''

        tx = self.graph.begin()
        for index, row in node_df.iterrows():
            tx.evaluate(merge_nodes, parameters={'l_login_a': row["participants"],
                                                 'l_repo': repo,
                                                 'l_owner': owner,
                                                 'l_r1': 'Touched',
                                                 'l_r2': 'Belongs_to'})
        tx.commit()

        print("{0}/{1}: Import to Neo4j succeeded!".format(owner, repo))
        print()

    def get_communities(self):

        query = "MATCH (n:USER) " \
                "WITH COUNT(n.community) AS count, n.community as community  " \
                "WHERE count > 1 " \
                "RETURN count, community " \
                "ORDER BY count DESC"

        return pd.DataFrame(self.graph.data(query))

    def get_degree(self):
        query = "MATCH r = (n:USER)-[x]-(m:USER) " \
                "RETURN Count(x) as node_degree, n.login as name"
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

    def export_graphjson(self):

        for e in conf.neo4j_export_json_pnames:

            repo = e["repo"]
            owner = e["owner"]

            print("exporting data in graphJSON format for {0}/{1}".format(owner, repo))

            # get all users who are attributed to owner/repo
            node_query = """MATCH (o:OWNER{name: $l_owner}) -- (r:REPO{ name: $l_repo })
                            WITH r
                            MATCH (r) -- (u:USER)
                            RETURN
                            id(u) AS id"""
            #               u.community AS group,
            #               u.login AS name

            link_query = """MATCH (o:OWNER{name: $l_owner}) -- (r:REPO{ name: $l_repo })
                            WITH r
                            MATCH (r) -- (u1:USER)
                            WITH u1
                            MATCH (u1) -[x:Mention|Quote|ContextualReply{owner: $l_owner, repo: $l_repo}]- (u2:USER)
                            RETURN id(u1) AS source,
                            id(u2) AS target,
                            x.weight AS weight,
                            type(x) AS rel_type,
                            x.timestamp AS timestamp,
                            id(x) AS link_id"""

            nodes = self.graph.data(node_query, parameters={'l_owner': owner, 'l_repo': repo})

            # communities = self.get_communities()
            # num_no_community = len(communities) + 1

            node_degree = self.get_degree()

            for node in nodes:
                # if node["group"] in communities["community"].values:
                #   idx = communities[communities["community"] == node["group"]].index[0]
                #   node["group"] = int(idx)
                #   node["hasGroup"] = True
                # else:
                #   node["group"] = num_no_community
                #   node["hasGroup"] = False

                if node["id"] in node_degree["name"].values:
                    idx = node_degree[node_degree["name"] == node["id"]].index[0]
                    node["degree_py"] = int(node_degree["node_degree"].iloc[idx])
                else:
                    node["degree_py"] = 0

            links = self.graph.data(link_query, parameters={'l_owner': owner, 'l_repo': repo})
            for link in links:
                link["weight"] = int(link["weight"])  # TODO: find the cause for weight being a string

            info = [{'owner': owner, 'repo': repo}]

            data = {"info": info, "nodes": nodes, "links": links}

            with open("Export/viz/data_{0}_{1}.json".format(owner, repo), "w") as fp:
                json.dump(data, fp, indent="\t")
