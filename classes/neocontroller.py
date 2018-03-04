"""
MODULE: neocontroller

This module handles all read and write transactions to the Neo4j database.

For a brief documentation on the Neo4j syntax see: https://neo4j.com/docs/cypher-refcard/current/

CLASSES:
    Neo4jController
"""
import json
from datetime import datetime

import pandas as pd
from py2neo import Graph

import conf as conf
# from analysis import Analyzer


class Neo4jController:
    def __init__(self):
        self.graph = Graph(user="max", password="1111")

    def import_references(self,
                          ref_df: pd.DataFrame):
        """
        Imports the passed references to neo4j.

        Can be activated/deactivated in the conf module

        :param ref_df:          DataFrame holding references with columns
                                    addressee_id
                                    comment_id
                                    ref_type
                                    thread_type
        :return:
        """

        if not conf.neo4j_import_references:
            return

        q_ref = '''MATCH (comment:COMMENT{id:$l_comment_id})
                    SET comment.thread_type = $l_thread_type
                    WITH comment
                    MATCH (target:USER{gha_id:$l_target_user_id})
                    CALL apoc.create.relationship(comment, $l_ref_type, {}, target) YIELD rel
                    RETURN rel
                    '''

        tx = self.graph.begin()
        for index, row in ref_df.iterrows():

            tx.evaluate(q_ref,
                        parameters={'l_login_b': row['addressee_id'],
                                    'l_comment_id': row['comment_id'],
                                    'l_ref_type': row['ref_type'],
                                    'l_thread_type': row['thread_type']})

            if (index + 1) % 10000 == 0:
                tx.commit()
                print("batch commit to neo4j at " + index)
                tx = self.graph.begin()
        tx.commit()

    def get_comment_timeframe(self,
                              repo_id: int):
        """
        Returns the dates of the repository's earliest and latest comment.

        :param repo_id:     repository id
        :return:            start- and enddate in 'yyyy-MM-dd' representation each
        """

        # TODO: c.tscomp does not exist in new neo4j structure
        # find a way to extract earliest date from timetree directly
        date_query = '''
                    MATCH (r:GHA_REPO)<--(c:COMMENT)
                    WHERE gha_id = $l_repo_id
                    UNWIND c.tscomp as ts
                    RETURN 
                    apoc.date.format(min(ts),'ms', 'yyyy-MM-dd') AS startdt, 
                    apoc.date.format(max(ts), 'ms', 'yyyy-MM-dd') AS enddt'''

        dates = self.graph.run(date_query, parameters={'l_repo_id': repo_id}).data()[0]

        startdt = datetime.strptime(dates["startdt"], "%Y-%m-%d").date()
        enddt = datetime.strptime(dates["enddt"], "%Y-%m-%d").date()

        return startdt, enddt

    def get_communication_subgraph(self,
                                   owner: str,
                                   repo: str,
                                   dt: pd.Timestamp) -> pd.DataFrame:
        """
        Queries a subgraph from the timetree consisting of communication links between users in a specific timeframe.

        The timeframe length can be configured in the conf module.

        :param owner:   repository owner
        :param repo:    repository name
        :param dt:      timestamp indicating the end of the desired period for the subgraph query
        :return:        pd.DataFrame containing links between nodes in the subgraph
        """

        # TODO: reformulate the range query as it might deliver huge results
        q_subgraph_time = '''
                            MATCH(r: GHA_REPO{gha_id: $l_repo_id})
                            WHERE(node: COMMENT)-->(r)
                            
                            WITH apoc.date.parse($l_dt, 'ms', 'yyyy-MM-dd') as end
                            WITH end, apoc.date.add(end, 'ms', $l_tf_length , 'd') as start
                            CALL ga.timetree.events.range({start: start, end: end}) YIELD node
                            
                            WITH node
                            
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



    def convert_keys_to_string(self, o):
        """
        Recursively converts dictionary keys to strings. Type conversion is required for exporting JSON files.

        :param o:      any object. If o is dictionary, method calls itself
        :return:       --
        """

        if not isinstance(o, dict):
            return o
        return dict((str(k), self.convert_keys_to_string(v))
                    for k, v in o.items())

    def export_graphjson(self):
        """
        Exports link data for single repositories as JSON files suitable to serve as input for  the gitNetViewer tool.

        Projects and export links to be exported can be configured in the conf module.

        The resulting JSON string has the following fields:

            "info":             a subset of repository related information fields containing
                                    owner:          owner name
                                    repo:           repository name
                                    total_links:    total number of links
                                    total_nodes:    total number of nodes
                                    no_threads:     total number of threads
                                    no_comments:    total number of comments

            "nodes":            a list of nodes with the fields
                                    name:       lowercase string representation of the users' login names
                                    id:         node id as provided by the database

            "links":            a list of links with the fields
                                    source:         the source node's id as provided by the database
                                    target:         the target node's id as provided by the database
                                    rel_type:       the relation type
                                    timestamp:      timestamp in the format 'yyyy-MM-dd'
                                    link_id:        link id as provided by the database

            "groups":           group attribution per node over time as provided by the Analyzer class
            "d_centrality":     degree centrality per node over time as provided by the Analyzer class
            "b_centrality":     betweenness centrality per node over time as provided by the Analyzer class
            "modularity":       modularity over time as provided by the Analyzer class

        :return:    --
        """
"""
        if not conf.neo4j_export_json:
            return

        for e in conf.neo4j_export_json_pnames:
            repo = e["repo"]
            owner = e["owner"]

            print("exporting data in graphJSON format for {0}/{1}".format(owner, repo))

            node_query = '''
                            MATCH (u:USER)<--(c:COMMENT)-->(r:GHA_REPO)-->(o:OWNER)
                            WHERE r.name = $l_repo and o.name = $l_owner
                            
                            WITH COLLECT({name: u.login, id: id(u)}) AS rows
                            MATCH (u:USER)-->(c:COMMENT)-->(r:GHA_REPO)-->(o:OWNER)
                            WHERE r.name = $l_repo and o.name = $l_owner
                            
                            WITH rows + COLLECT({name: u.login, id: id(u)}) as allRows
                            UNWIND allRows as row
                            
                            WITH row.name as name, row.id as id
                            RETURN DISTINCT name, id
                            '''

            link_query = '''MATCH (r:GHA_REPO) --> (o:OWNER)
                            WHERE r.name = $l_repo
                            and o.name = $l_owner
                            
                            WITH DISTINCT r
                            MATCH (c:COMMENT)-->(r)
                            
                            WITH DISTINCT c
                            MATCH (c)-[x]->(target:USER)
                            MATCH (source:USER)-->(c)
                            
                            WITH DISTINCT x, source, target, c
                            RETURN 
                            id(source) as source,
                            id(target) as target,
                            type(x) as rel_type,
                            apoc.date.format(c.tscomp, 'ms', 'yyyy-MM-dd') AS timestamp,
                            id(x) as link_id
                            '''

            nodes = self.graph.data(node_query, parameters={'l_owner': owner, 'l_repo': repo})
            links = self.graph.data(link_query, parameters={'l_owner': owner, 'l_repo': repo})

            info_query = '''MATCH (r:GHA_REPO)-->(o:OWNER)
                            WHERE r.name = $l_repo and o.name = $l_owner
                            RETURN 
                            r.no_comments as no_comments,  
                            r.no_threads as no_threads'''

            # TODO: no_comments and no_threads has been removed from :GHA_REPO. Replace this with
            # comment count instead.

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

            with open("Export_Network/viz/data_{0}_{1}.json".format(owner, repo), "w") as fp:
                json.dump(data, fp, indent="\t")
"""