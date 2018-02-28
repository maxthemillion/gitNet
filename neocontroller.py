"""
MODULE: neocontroller

This module handles all read and write transactions to the Neo4j database.
The database makes use of a timetree which is defined to resolute down to days. Timestamps are converted to integers
representing milliseconds since Jan 1st, 1970 using UTC timezone.

For a brief documentation on the Neo4j syntax see: https://neo4j.com/docs/cypher-refcard/current/

CLASSES:
    Neo4jController
"""

from py2neo import Graph
import json
import pandas as pd
import warnings
import conf
from datetime import datetime
from analysis import Analyzer


class Neo4jController:
    def __init__(self):
        self.graph = Graph(user="max", password="1111")
        # TODO: collect paths as object attributes

    def import_repo(self,
                    ref_df: pd.DataFrame,
                    node_df: pd.DataFrame,
                    owner: str,
                    repo: str,
                    stats):
        """
        Imports the passed references and nodes to the database and relates them to the passed owner and repository.
        First matches owner, repository and their relationship. Then adds the repository's comments to the timetree
        and their relationships to users.

        Can be activated/deactivated in the conf module.

        :param ref_df:      pd.DataFrame containing references
        :param node_df:     pd.DataFrame containing participants in a comment thread
        :param owner:       repository owner
        :param repo:        repository name
        :return:            --
        """

        if not conf.neo4j_import:
            return

        #TODO: this query merges repos with the same name which is wrong because there can be more than one repo with the same name
        # ! change to pattern MERGE (r:REPO{name})-->(o:OWNER{name})
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
                        MERGE  (comment)-[:to]->(r)
                        RETURN r 
                        '''

        tx = self.graph.begin()
        for index, row in ref_df.iterrows():

            tx.evaluate(q_comments,
                        parameters={'l_login_a': row['commenter'],
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

    def get_timeframe(self,
                      owner: str,
                      repo: str):
        """
        Returns the dates of the repository's earliest and latest comment.

        :param owner:   repository owner
        :param repo:    repository name
        :return:        start- and enddate in 'yyyy-MM-dd' representation each
        """

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
        # TODO: check this query. Does it deliver what you expect?
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
                            RETURN 
                            id(source) as source,
                            id(target) as target,
                            type(x) as rel_type,
                            apoc.date.format(c.tscomp, 'ms', 'yyyy-MM-dd') AS timestamp,
                            id(x) as link_id
                            '''

            nodes = self.graph.data(node_query, parameters={'l_owner': owner, 'l_repo': repo})
            links = self.graph.data(link_query, parameters={'l_owner': owner, 'l_repo': repo})

            info_query = '''MATCH (r:REPO)-->(o:OWNER)
                            WHERE r.name = $l_repo and o.name = $l_owner
                            RETURN 
                            r.no_comments as no_comments, 
                            r.no_threads as no_threads'''

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

    def import_commits(self,
                       commits: pd.DataFrame,
                       owner: str,
                       repo: str):
        """
        Imports the passed commits and relates them to the passed owner and repository

        :param commits:        pd.DataFrame containing commits with columns
                                    login:          username of the commit's author
                                    commit_id:      the commit's id as provided by GHTorrent
                                    created_at:     a timestamp dating to the commits creation

        :param owner:          owner name
        :param repo:           repository name
        :return:               --
        """

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

    def import_issues(self,
                      issues: pd.DataFrame,
                      owner: str,
                      repo: str):
        """
        Imports passed issues and connects them to passed owner and repository

        :param issues:      pd.DataFrame containing issues with columns
                                reporter:       username of the issue's reporter
                                issue_id:       the issue's id as provided by GHTorrent
                                created_at:     timestamp dating to the issue's creation
        :param owner:       owner name
        :param repo:        repository name
        :return:
        """

        q_commits = '''
                     WITH apoc.date.parse($l_time, 'ms', 'yyyy-MM-dd') as dt
                     MERGE (c:ISSUE{id:$l_id})
                     SET c.tscomp = dt, c.id = $l_id
                     
                     WITH c, dt
                     CALL ga.timetree.events.attach({node: c, time: dt, relationshipType: "CreatedOn"}) 
                     YIELD node as issue
                     
                     WITH issue
                     MERGE (u:USER{login:$l_login})
                     MERGE (u)-[:raises]->(issue)
                     
                     WITH issue
                     MERGE (r:REPO{name:$l_repo}) -[:BelongsTo]-> (o:OWNER{name:$l_owner})
                     MERGE  (issue)-[:to]-> (r)                    
                     RETURN r
                     '''

        tx = self.graph.begin()
        for index, row in issues.iterrows():

            tx.evaluate(q_commits, parameters={'l_login': row['reporter'],
                                               'l_id': row['issue_id'],
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

