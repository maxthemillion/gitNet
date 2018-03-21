"""
MODULE: neocontroller

This module handles all read and write transactions to the Neo4j database.

For a brief documentation on the Neo4j syntax see: https://neo4j.com/docs/cypher-refcard/current/

CLASSES:
    Neo4jController
"""
from datetime import datetime

import pandas as pd
from py2neo import Graph

import conf as conf


class Neo4jController:
    def __init__(self):
        self.graph = Graph(user="max", password="1111")

    def get_comment_timeframe(self,
                              repo_id: int):
        """
        Returns the dates of the repository's earliest and latest comment.

        :param repo_id:     repository id
        :return:            start- and enddate in 'yyyy-MM-dd' representation each
        """

        date_query = '''
                    MATCH (r:REPO{ght_id:$l_repo_id})
                    MATCH (comment)-[:to]->(x)-[:to]->(r)
                    WITH comment.event_time as ts
                    RETURN 
                    apoc.date.format(min(ts),'ms', 'yyyy-MM-dd') AS startdt, 
                    apoc.date.format(max(ts), 'ms', 'yyyy-MM-dd') AS enddt'''

        dates = self.graph.run(date_query, parameters={'l_repo_id': repo_id}).data()[0]

        startdt = datetime.strptime(dates["startdt"], "%Y-%m-%d").date()
        enddt = datetime.strptime(dates["enddt"], "%Y-%m-%d").date()

        return startdt, enddt

    def get_communication_subgraph(self,
                                   owner: str,
                                   repo: int,
                                   dt: pd.Timestamp) -> pd.DataFrame:
        """
        Queries a subgraph from the timetree consisting of communication links between users in a specific timeframe.

        The timeframe length can be configured in the conf module.

        :param owner:   repository owner
        :param repo:    repository name
        :param dt:      timestamp indicating the end of the desired period for the subgraph query
        :return:        pd.DataFrame containing links between nodes in the subgraph
        """

        q_subgraph_time = '''            
            MATCH (o:OWNER{login:$l_owner})
            MATCH (r:REPO{ght_id:$l_repo}) -[:belongs_to]-> (o)
            
            WITH
                r,
                apoc.date.parse($l_dt, 'ms', 'yyyy-MM-dd') as end
            WITH
                r, 
                end, 
                apoc.date.add(end, 'ms', $l_tf_length , 'd') as start

            
            MATCH (node:COMMENT) -[:to]-> () -[:to]-> (r)
            WHERE node.event_time >= start AND node.event_time <= end
            WITH node as comment 
                    
            MATCH (source:USER) -[:makes]-> (comment) -[x]-> (target:USER)
            WITH DISTINCT 
                source, 
                target          
            
            RETURN 
            id(source) as source,
            id(target) as target
        '''

        links = pd.DataFrame(self.graph.data(q_subgraph_time,
                                             parameters={"l_owner": owner,
                                                         "l_repo": repo,
                                                         "l_tf_length": (-1 * conf.a_length_timeframe),
                                                         "l_dt": dt.strftime("%Y-%m-%d")}
                                             ))
        return links

    def get_viz_data(self,
                                            owner: str,
                                            repo: int):
        """
        Returns data required to visualize a single repository out of a project with the NetViz tool.

        :param owner:       Owner name
        :param repo:        ght_id of a desired repository
        :return:
            nodes           format description see export_graphjson method in the analysis module
            links           -"-
            info            -"-

        """

        node_query = '''
            MATCH (o:OWNER{login:$l_owner})
            MATCH (r:REPO{ght_id:16727251}) -[:belongs_to]-> (o)

            MATCH (comment) -[:to]-> (x) -[:to]-> (r)
        
            MATCH (source:USER) -[:makes]-> (comment)
            
            WITH comment, COLLECT (DISTINCT source) as users
            MATCH (comment) --> (target:USER)
            WITH users + COLLECT (DISTINCT target) as users
            
            UNWIND users as u
            
            RETURN DISTINCT u.gha_id as name, id(u) as id
            '''

        link_query = '''
            MATCH (o:OWNER{login:$l_owner})
            MATCH (r:REPO{ght_id:$l_repo_id}) -[:belongs_to]-> (o)
            
            MATCH (comment) -[:to]-> (x) -[:to]-> (r)
            WITH comment
                    
            MATCH (source:USER) -[:makes]-> (comment) -[x]-> (target:USER)
            WITH DISTINCT 
                source, 
                x, 
                target,             
                apoc.date.format(comment.event_time, 'ms', 'yyyy-MM-dd') AS timestamp
            
            RETURN 
            id(source) as source,
            id(target) as target,
            timestamp,
            type(x) as rel_type,
            id(x) as link_id            
            '''

        info_query = '''
            MATCH (o:OWNER{login:$l_owner})
            MATCH (r:REPO{ght_id:$l_repo_id}) -[:belongs_to]-> (o)
            MATCH (comment) -[:to]-> (x) -[:to]-> (r)
            RETURN COUNT(DISTINCT comment) as no_comments, -1 as no_threads
            '''

        nodes = self.graph.data(node_query, parameters={'l_owner': owner, 'l_repo_id': repo})
        links = self.graph.data(link_query, parameters={'l_owner': owner, 'l_repo_id': repo})
        info_res = self.graph.data(info_query, parameters={'l_owner': owner, 'l_repo_id': repo})[0]

        info = [{'owner': owner,
                 'repo': repo,
                 'total_nodes': len(nodes),
                 'total_links': len(links),
                 'no_threads': info_res["no_threads"],
                 'no_comments': info_res["no_comments"]
                 }]

        return nodes, links, info

