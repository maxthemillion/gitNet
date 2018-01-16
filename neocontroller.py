from py2neo import Graph
import json
import pandas as pd
import warnings
import conf
from dateutil import rrule
from datetime import datetime, timedelta
import time
import networkx as nx
import community as nxlouvain


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
                    WITH rel
                    SET rel.tscomp = apoc.date.parse(rel.timestamp, 's',"yyyy-MM-dd'T'HH:mm:ss")
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

        parse_dates = '''MATCH (n:USER)-[x]-(m:USER)
                        WITH x
                        RETURN CALL apoc.date.parse(x.timestamp, 's',"yyyy-MM-dd'T'HH:mm:ss") as parsed
                        '''

        print("{0}/{1}: Import to Neo4j succeeded!".format(owner, repo))
        print()

    def get_degree(self):
        query = "MATCH r = (n:USER)-[x]-(m:USER) " \
                "RETURN Count(x) as node_degree, n.login as name"
        nodes = pd.DataFrame(self.graph.data(query))
        return nodes

    def louvain_networkx(self, owner, repo):
        date_query = '''MATCH (u:USER) -[x]-> (u2:USER) 
                                    WHERE x.owner = "{0}" and x.repo = "{1}"
                                    UNWIND x.tscomp as ts
                                    RETURN 
                                    apoc.date.format( min(ts),'s', 'yyyy-MM-dd') AS startdt, 
                                    apoc.date.format(max(ts), 's', 'yyyy-MM-dd') AS enddt'''.format(owner, repo)

        dates = self.graph.run(date_query).data()[0]

        startdt = datetime.strptime(dates["startdt"], "%Y-%m-%d").date()
        enddt = datetime.strptime(dates["enddt"], "%Y-%m-%d").date()

        print("Running NX Louvain algorithm for {0}/{1} and timeframe length {2}".format(owner, repo,
                                                                                         conf.neo4j_length_timeframe))
        res = []
        for dt in rrule.rrule(rrule.WEEKLY, dtstart=startdt, until=enddt):
            time_start = time.time()

            squery_links = '''WITH apoc.date.parse($l_dt, 's', "yyyy-MM-dd") AS dateEnd 
                                      WITH dateEnd,  apoc.date.add(dateEnd, 's', $l_tf_length, 'd') AS dateStart
                                      MATCH (u1:USER)-[x]->(u2:USER)
                                      WHERE x.owner = $l_owner
                                      and x.repo = $l_repo
                                      and x.tscomp < dateEnd
                                      and x.tscomp > dateStart
                                      RETURN id(u1) as source, id(u2) as target'''

            links = pd.DataFrame(self.graph.run(squery_links,
                                                parameters={"l_owner": owner,
                                                            "l_repo": repo,
                                                            "l_tf_length": conf.neo4j_length_timeframe * -1,
                                                            "l_dt": dt.strftime("%Y-%m-%d")}
                                                ).data())

            if not links.empty:
                nxgraph = nx.from_pandas_dataframe(links, source="source", target="target")

                partition = nxlouvain.best_partition(nxgraph)

                partition = Neo4jController.convert_keys_to_string(partition)

                res.append({dt.strftime("%Y-%m-%d"): partition})

            print("current: {0} - time: {1}".format(dt.date(), time.time() - time_start))

        return res

    def run_louvain_on_subgraph(self, owner, repo):
        warnings.warn("buggy louvain implementation on Neo4j. "
                      "louvain runs on complete graph despite selection of subgraph")

        date_query = '''MATCH (u:USER) -[x]-> (u2:USER) 
                            WHERE x.owner = "{0}" and x.repo = "{1}"
                            UNWIND x.tscomp as ts
                            RETURN 
                            apoc.date.format( min(ts),'s', 'yyyy-MM-dd') AS startdt, 
                            apoc.date.format(max(ts), 's', 'yyyy-MM-dd') AS enddt'''.format(owner, repo)

        dates = self.graph.run(date_query).data()[0]

        startdt = datetime.strptime(dates["startdt"], "%Y-%m-%d").date()
        enddt = datetime.strptime(dates["enddt"], "%Y-%m-%d").date()

        startdt = datetime.strptime("2016-09-01", "%Y-%m-%d").date()

        print("Running Louvain algorithm for {0}/{1} and timeframe length {2}".format(owner, repo,
                                                                                      conf.neo4j_length_timeframe))
        res = []
        for dt in rrule.rrule(rrule.WEEKLY, dtstart=startdt, until=enddt):
            time_start = time.time()

            squery_nodes = '''WITH apoc.date.parse('2016-09-01', 's', 'yyyy-MM-dd') AS dateEnd 
                              WITH dateEnd,  apoc.date.add(dateEnd, 's', -30, 'd') AS dateStart
                              MATCH (u:USER) -[x]- (u2:USER) 
                              WHERE x.owner = {0}
                              and x.repo = {1}
                              and x.tscomp < dateEnd
                              and x.tscomp > dateStart
                              RETURN Distinct id(u) as id'''.format(owner, repo)

            squery_links = '''WITH apoc.date.parse({0}, 's', "yyyy-MM-dd") AS dateEnd 
                              WITH dateEnd,  apoc.date.add(dateEnd, 's', {1}, 'd') AS dateStart
                              MATCH (u1:USER)-[x]->(u2:USER)
                              WHERE x.owner = {2}
                              and x.repo = {3}
                              and x.tscomp < dateEnd
                              and x.tscomp > dateStart
                              RETURN id(u1) as source, id(u2) as target''' \
                .format(dt, conf.neo4j_length_timeframe * -1, owner, repo)

            query = '''CALL algo.louvain.stream(
                        $l_node_query, 
                        $l_link_query,   
                        {graph:'cypher', concurrency:4})
                        YIELD nodeId, community
                        RETURN nodeId as id, community as group                      
                    '''

            res.append({dt.strftime("%Y-%m-%d"): self.graph.run(query,
                                                                parameters=({'l_node_query': squery_nodes,
                                                                             'l_link_query': squery_links})).data()})
            print("current: {0} - time: {1}".format(dt.date(), time.time() - time_start))

        return res

    def stream_to_gephi(self):
        print("Streaming network to Gephi...")
        query_part = "MATCH path = (:USER)--(:USER)" \
                     "CALL apoc.gephi.add(null, 'workspace1', path, 'weight', ['community']) " \
                     "YIELD nodes " \
                     "return *"
        self.graph.run(query_part)
        print("Complete!")
        print()

    @staticmethod
    def convert_keys_to_string(dictionary):
        """Recursively converts dictionary keys to strings."""
        if not isinstance(dictionary, dict):
            return dictionary
        return dict((str(k), Neo4jController.convert_keys_to_string(v))
                    for k, v in dictionary.items())

    def export_graphjson(self):

        if not conf.neo4j_export_json:
            return

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

            node_degree = self.get_degree()

            for node in nodes:

                if node["id"] in node_degree["name"].values:
                    idx = node_degree[node_degree["name"] == node["id"]].index[0]
                    node["degree_py"] = int(node_degree["node_degree"].iloc[idx])
                else:
                    node["degree_py"] = 0

            links = self.graph.data(link_query, parameters={'l_owner': owner, 'l_repo': repo})
            for link in links:
                link["weight"] = int(link["weight"])  # TODO: find the cause for weight being a string

            info = [{'owner': owner,
                     'repo': repo,
                     'total_nodes': len(nodes),
                     'total_links': len(links)}]

            groups = []
            if conf.neo4j_run_louvain:
                groups = self.louvain_networkx(owner, repo)

            data = {"info": info, "nodes": nodes, "links": links, "groups": groups}

            with open("Export/viz/data_{0}_{1}.json".format(owner, repo), "w") as fp:
                json.dump(data, fp, indent="\t")
