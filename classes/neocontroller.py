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

        self.dev_core = None
        self.dev_core_owner = None

    def get_comment_timeframe(self,
                              owner: str,
                              repo_id=None):
        """
        Returns the dates of the repository's earliest and latest comment.

        :param owner:       owner name
        :param repo_id:     repository id. if not supplied, will consider all repos which belong to the specified owner
        :return:            start- and enddate in 'yyyy-MM-dd' representation each
        """
        if repo_id is None:
            date_query = '''
                            MATCH (o:OWNER{login:$l_owner})
                            MATCH (:USER)-[:makes]->(comment)-[:to]->(x)-[:to]->(r)-[:belongs_to]->(o)
                            WITH comment.event_time as ts
                            RETURN 
                            apoc.date.format(min(ts),'ms', 'yyyy-MM-dd') AS startdt, 
                            apoc.date.format(max(ts), 'ms', 'yyyy-MM-dd') AS enddt'''

            dates = self.graph.run(date_query, parameters={'l_owner': owner}).data()[0]

            startdt = datetime.strptime(dates["startdt"], "%Y-%m-%d").date()
            enddt = datetime.strptime(dates["enddt"], "%Y-%m-%d").date()

        else:
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

    def get_dev_core(self,
                     owner: str):
        """
        Retrieves the developer core of a project. The core is identified via simple criteria:
         1. contributions:
        :param owner:
        :return:
        """

        if self.dev_core_owner == owner:
            dev_core = self.dev_core
        else:

            q_count_contribs = """MATCH(o: OWNER{login: $l_owner})
            WITH o
            // comments
            MATCH(u: USER)-[: makes]->(node: COMMENT)-[: to]->() - [: to]->() - [: belongs_to]->(o)
            WITH
            o, COLLECT({u_id: u.gha_id, n_id: id(node)}) as comments
    
            // technicals
            MATCH(u: USER)-->(node) - [: to]-> () - [: belongs_to]-> (o)
            WHERE (node: PULLREQUEST OR node: ISSUE OR node: COMMIT)
            WITH
            comments + COLLECT({u_id: id(u), n_id: id(node)}) as allContributions
            UNWIND
            allContributions as row
    
            WITH
            row.u_id as u_id, COUNT(DISTINCT row.n_id) as count_contributions
            WHERE
            count_contributions >= $l_min_contributions
            RETURN
            u_id as u_id;"""

            dev_core = pd.DataFrame(self.graph.data(q_count_contribs,
                                                    parameters={"l_owner": owner,
                                                                "l_min_contributions": conf.a_dev_core_min_contributions}
                                                    ))
            self.dev_core_owner = owner
            self.dev_core = dev_core

        return dev_core

    def get_communication_subgraph(self,
                                   owner: str,
                                   dt: pd.Timestamp
                                   ) -> pd.DataFrame:
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

            WITH
                o,
                apoc.date.parse($l_dt, 'ms', 'yyyy-MM-dd') as end
            WITH
                o, 
                end, 
                apoc.date.add(end, 'ms', $l_tf_length , 'd') as start


            MATCH (node:COMMENT) -[:to]-> () -[:to]-> (r) -[:belongs_to]-> (o)
            WHERE node.event_time >= start AND node.event_time <= end
            WITH node as comment 

            MATCH (source:USER) -[:makes]-> (comment) -[x]-> (target:USER)
            WHERE id(source) <> id(target)
            
            WITH DISTINCT 
                source, 
                target          

            RETURN 
            id(source) as source,
            id(target) as target
        '''

        links = pd.DataFrame(self.graph.data(q_subgraph_time,
                                             parameters={"l_owner": owner,
                                                         "l_tf_length": (-1 * conf.a_length_timeframe),
                                                         "l_dt": dt.strftime("%Y-%m-%d")}
                                             ))

        if conf.a_filter_core and not links.empty:
            dev_core = self.get_dev_core(owner)

            links = links[(links['source'].isin(dev_core['u_id']) &
                           links['target'].isin(dev_core['u_id']))]

        return links

    def get_viz_data(self,
                     owner: str):
        """
        Returns data required to visualize a project with the NetViz tool.

        :param owner:       Owner name
        :param repo:        ght_id of a desired repository. if not provided, the function returns data for all repos associated
                            with the project
        :return:
            nodes           format description see export_graphjson method in the analysis module
            links           -"-
            info            -"-

        """

        node_query = '''
            MATCH (o:OWNER{login:$l_owner})

            MATCH (comment) -[:to]-> (x) -[:to]-> (r) -[:belongs_to] -> (o)

            MATCH (source:USER) -[:makes]-> (comment)

            WITH comment, COLLECT (DISTINCT source) as users
            MATCH (comment) --> (target:USER)
            WITH users + COLLECT (DISTINCT target) as users

            UNWIND users as u

            RETURN DISTINCT u.gha_id as name, id(u) as id
            '''

        link_query = '''
            MATCH (o:OWNER{login:$l_owner})

            MATCH (comment) -[:to]-> (x) -[:to]-> (r) -[:belongs_to]-> (o)
            WITH comment

            MATCH (source:USER) -[:makes]-> (comment) -[x]-> (target:USER)
            WHERE id(source) <> id(target)

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
            MATCH (comment) -[:to]-> (x) -[:to]-> (r) -[:belongs_to]-> (o)
            RETURN COUNT(DISTINCT comment) as no_comments, -1 as no_threads
            '''

        nodes = self.graph.data(node_query, parameters={'l_owner': owner, 'l_repo_id': -1})
        links = self.graph.data(link_query, parameters={'l_owner': owner, 'l_repo_id': -1})

        dev_core = self.get_dev_core(owner)

        if conf.a_filter_core and links:

            links = pd.DataFrame(links)
            links = links[(links['source'].isin(dev_core['u_id']) &
                           links['target'].isin(dev_core['u_id']))]
            links = links.to_dict('records')

        info_res = self.graph.data(info_query, parameters={'l_owner': owner, 'l_repo_id': -1})[0]

        info = [{'owner': owner,
                 'repo': " ",
                 'total_nodes': len(nodes),
                 'size_core': len(dev_core),
                 'total_links': len(links),
                 'no_threads': info_res["no_threads"],
                 'no_comments': info_res["no_comments"]
                 }]

        return nodes, links, info

