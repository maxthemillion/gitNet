from py2neo import Graph
import json
import pandas as pd
import warnings
import conf
from dateutil import rrule
from datetime import datetime
import time
from analysis import Analyzer


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

    def import_project(self, ref_df, node_df, owner, repo, stats):
        """First creates owner and repository relationship. Then adds comments to the timetree
        and their relationships to users."""

        if not conf.neo4j_import:
            return

        q_repos = '''MERGE(r:REPO{name:$l_repo})
                        SET r.no_threads = $l_no_threads
                        SET r.no_comments = $l_no_comments
                        WITH r
                      MERGE (o:OWNER{name:$l_owner})
                      WITH r, o
                      MERGE (r)-[y:BelongsTo]->(o)
                      RETURN y'''

        self.graph.run(q_repos, parameters={'l_repo': repo,
                                            'l_owner': owner,
                                            'l_no_comments': stats.get_no_comments(),
                                            'l_no_threads': stats.get_no_threads()})

        q_comments = '''WITH apoc.date.parse($l_time, 'ms', 'yyyy-MM-dd') as dt
                        MERGE (c:COMMENT{id:$l_comment_id})
                        WITH dt, c
                        CALL ga.timetree.events.attach({node: c, time: dt, relationshipType: "CreatedOn"}) 
                        YIELD node as comment
                        WITH comment, dt
                        SET comment.thread_type = $l_thread_type
                        SET comment.tscomp = dt
                        WITH comment
                        MERGE (a:USER{login:$l_login_a})
                        MERGE (b:USER{login:$l_login_b})
                        WITH comment, a, b
                        MERGE (a) -[:makes]->(comment)
                        WITH comment, b
                        CALL apoc.create.relationship(comment, $l_ref_type, {}, b) YIELD rel as rel2
                        WITH comment
                        MATCH (r:REPO{name:$l_repo_name}) --> (o:OWNER{name:$l_owner_name})
                        MERGE  (comment)-[:to]-> (r)
                        RETURN r 
                        '''

        tx = self.graph.begin()
        for index, row in ref_df.iterrows():

            tx.evaluate(q_comments, parameters={'l_login_a': row['commenter'],
                                                'l_login_b': row['addressee'],
                                                'l_ref_type': row['ref_type'],
                                                'l_time': row['timestamp'].strftime("%Y-%m-%d"),
                                                'l_owner_name': owner,
                                                'l_repo_name': repo,
                                                'l_thread_type': row['thread_type'],
                                                'l_comment_id': row['comment_id']})
            if (index + 1) % 10000 == 0:
                tx.commit()
                warnings.warn("batch commit to neo4j at " + index)
                tx = self.graph.begin()
        tx.commit()

        print("{0}/{1}: Import to Neo4j succeeded!".format(owner, repo))
        print()

    def get_degree(self):
        query = "MATCH r = (n:USER)-[x]-(m:USER) " \
                "RETURN Count(x) as node_degree, n.login as name"
        nodes = pd.DataFrame(self.graph.data(query))
        return nodes

    def get_timeframe(self, owner, repo):

        date_query = '''
                    MATCH (o:OWNER)<--(r:REPO)<--(c:COMMENT)
                    WHERE o.name = $l_owner and r.name = $l_repo
                    UNWIND c.tscomp as ts
                    RETURN 
                    apoc.date.format(min(ts),'ms', 'yyyy-MM-dd') AS startdt, 
                    apoc.date.format(max(ts), 'ms', 'yyyy-MM-dd') AS enddt'''

        dates = self.graph.run(date_query, parameters={'l_owner': owner, 'l_repo': repo}).data()[0]

        startdt = datetime.strptime(dates["startdt"], "%Y-%m-%d").date()
        enddt = datetime.strptime(dates["enddt"], "%Y-%m-%d").date()

        return startdt, enddt

    def get_subgraph(self, owner, repo, dt):

        q_subgraph_time = '''
                            WITH apoc.date.parse($l_dt, 'ms', 'yyyy-MM-dd') as end
                            WITH end, apoc.date.add(end, 'ms', $l_tf_length , 'd') as start
                            CALL ga.timetree.events.range({start: start, end: end}) YIELD node
                            WITH node
                            MATCH(r: REPO{name: $l_repo})-->(o: OWNER{name: $l_owner})
                            WHERE(node: COMMENT)-->(r)
                            WITH DISTINCT node as comment
                            MATCH (source:USER) --> (comment)
                            MATCH (comment) --> (target:USER)
                            RETURN source.login as source, target.login as target
                            '''

        links = pd.DataFrame(self.graph.data(q_subgraph_time,
                                             parameters={"l_owner": owner,
                                                         "l_repo": repo,
                                                         "l_tf_length": (-1 * conf.a_length_timeframe),
                                                         "l_dt": dt.strftime("%Y-%m-%d")}
                                             ))

        return links

    def stream_to_gephi(self):
        print("Streaming network to Gephi...")
        query_part = "MATCH path = (:USER)--(:USER)" \
                     "CALL apoc.gephi.add(null, 'workspace1', path, 'weight', ['community']) " \
                     "YIELD nodes " \
                     "return *"
        self.graph.run(query_part)
        print("Complete!")
        print()

    def convert_keys_to_string(self, dictionary):
        """Recursively converts dictionary keys to strings."""
        if not isinstance(dictionary, dict):
            return dictionary
        return dict((str(k), self.convert_keys_to_string(v))
                    for k, v in dictionary.items())

    def export_graphjson(self):

        if not conf.neo4j_export_json:
            return

        for e in conf.neo4j_export_json_pnames:
            repo = e["repo"]
            owner = e["owner"]

            print("exporting data in graphJSON format for {0}/{1}".format(owner, repo))

            node_query = '''
                            MATCH (u:USER)<--(c:COMMENT)-->(r:REPO)-->(o:OWNER)
                            WHERE r.name = $l_repo and o.name = $l_owner
                            WITH COLLECT({name: u.login, id: id(u)}) AS rows
                            MATCH (u:USER)-->(c:COMMENT)-->(r:REPO)-->(o:OWNER)
                            WHERE r.name = $l_repo and o.name = $l_owner
                            WITH rows + COLLECT({name: u.login, id: id(u)}) as allRows
                            UNWIND allRows as row
                            WITH row.name as name, row.id as id
                            RETURN DISTINCT name, id
                            '''

            link_query = '''MATCH (r:REPO) --> (o:OWNER)
                            WHERE r.name = $l_repo
                            and o.name = $l_owner
                            WITH DISTINCT r
                            MATCH (c:COMMENT)-->(r)
                            WITH DISTINCT c
                            MATCH (c)-[x]->(target:USER)
                            MATCH (source:USER)-->(c)
                            WITH DISTINCT x, source, target, c
                            RETURN id(source) as source,
                            id(target) as target,
                            type(x) as rel_type,
                            apoc.date.format(c.tscomp, 'ms', 'yyyy-MM-dd') AS timestamp,
                            id(x) as link_id
                            '''

            nodes = self.graph.data(node_query, parameters={'l_owner': owner, 'l_repo': repo})
            links = self.graph.data(link_query, parameters={'l_owner': owner, 'l_repo': repo})

            info_query = '''MATCH (r:REPO)-->(o:OWNER)
                            WHERE r.name = $l_repo and o.name = $l_owner
                            RETURN r.no_comments as no_comments, r.no_threads as no_threads'''

            info_res = self.graph.data(info_query, parameters={'l_repo': repo,
                                                               'l_owner': owner})[0]

            info = [{'owner': owner,
                     'repo': repo,
                     'total_nodes': len(nodes),
                     'total_links': len(links),
                     'no_threads': info_res["no_threads"],
                     'no_comments': info_res["no_comments"]
                     }]

            a = Analyzer(owner, repo)
            a.run()

            groups = self.convert_keys_to_string(a.get_groups())
            d_centrality = self.convert_keys_to_string(a.get_degree_centrality())
            b_centrality = self.convert_keys_to_string(a.get_betweenness_centrality())
            modularity = self.convert_keys_to_string(a.get_modularity())

            data = {"info": info,
                    "nodes": nodes,
                    "links": links,
                    "groups": groups,
                    "d_centrality": d_centrality,
                    "b_centrality": b_centrality,
                    "modularity": modularity}

            with open("Export/viz/data_{0}_{1}.json".format(owner, repo), "w") as fp:
                json.dump(data, fp, indent="\t")

    def import_commits(self, commits, owner, repo):

        q_commits = '''
                    WITH apoc.date.parse($l_time, 'ms', 'yyyy-MM-dd') as dt
                    MERGE (c:COMMIT{id:$l_id})
                    SET c.tscomp = dt, c.id = $l_id
                    WITH c, dt
                    CALL ga.timetree.events.attach({node: c, time: dt, relationshipType: "CreatedOn"}) 
                    YIELD node as commit
                    WITH commit
                    MERGE (u:USER{login:$l_login})
                    MERGE (u)-[:commits]->(commit)
                    WITH commit
                    MERGE (r:REPO{name:$l_repo}) -[:BelongsTo]-> (o:OWNER{name:$l_owner})
                    MERGE  (commit)-[:to]-> (r)                    
                    RETURN r
                    '''

        tx = self.graph.begin()
        for index, row in commits.iterrows():

            tx.evaluate(q_commits, parameters={'l_login': row['login'],
                                               'l_id': row['commit_id'],
                                               'l_time': row['created_at'].strftime("%Y-%m-%d"),
                                               'l_owner': owner,
                                               'l_repo': repo})
            if (index + 1) % 10000 == 0:
                tx.commit()
                warnings.warn("batch commit to neo4j at " + index)
                tx = self.graph.begin()
        tx.commit()

        print("{0}/{1}: Import to Neo4j succeeded!".format(owner, repo))
        print()
