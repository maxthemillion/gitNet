import time
import conf

import community as nxlouvain
# import matplotlib.pyplot as plt
import networkx as nx
import pandas as pd
from dateutil import rrule
import json

from classes.neocontroller import Neo4jController


owners = ['OneDrive']
repos = [16727251]


def analyze_repos():

    for owner in owners:
        for repo in repos:
            analyzer = Analyzer(owner, repo)
            analyzer.export_graphjson()


class Analyzer:
    def __init__(self,
                 owner: str,
                 repo: int):
        self._owner = owner
        self._repo = repo

        self._controller = Neo4jController()

        self._startdt, self._enddt = self._controller.get_comment_timeframe(self._repo)

        self._degree_centrality = None
        self._betweenness_centrality = None
        self._eigenvector_centrality = None
        self._partition = None
        self._modularity = None

        self.run()

    def run(self):
        self._individual_measures()
        self._export_measures()

    def get_groups(self):
        return self._partition

    def get_betweenness_centrality(self):
        return self._betweenness_centrality

    def get_degree_centrality(self):
        return self._degree_centrality

    def get_modularity(self):
        return self._modularity

    def _louvain_networkx(self):

        print("Running NX Louvain algorithm for {0}/{1} and timeframe length {2}".format(self._owner, self._repo,
                                                                                         conf.a_length_timeframe))
        res = {}
        for dt in rrule.rrule(rrule.WEEKLY, dtstart=self._startdt, until=self._enddt):
            time_start = time.time()

            links = self._controller.get_communication_subgraph(self._owner, self._repo, dt)

            if not links.empty:
                nxgraph = nx.from_pandas_dataframe(links, source="source", target="target", create_using=nx.MultiGraph())

                partition = nxlouvain.best_partition(nxgraph)

                # partition = self.convert_keys_to_string(partition)

                res[dt.strftime("%Y-%m-%d")] = partition

            print("current: {0} - time: {1}".format(dt.date(), time.time() - time_start))

        return res

    def _individual_measures(self):

        print("Running NX analysis for {0}/{1} and timeframe length {2}".format(self._owner,
                                                                                self._repo,
                                                                                conf.a_length_timeframe))
        res_louvain = {}
        res_degree_centrality = {}
        res_betweenness_centrality = {}
        res_eigenvector_centrality = {}
        res_modularity = {}

        time_start = time.time()
        for dt in rrule.rrule(rrule.WEEKLY, dtstart=self._startdt, until=self._enddt):
            lap_time = time.time()

            links = self._controller.get_communication_subgraph(self._owner, self._repo, dt)

            if not links.empty:
                multi_graph = nx.from_pandas_dataframe(links,
                                                       source="source",
                                                       target="target",
                                                       create_using=nx.MultiGraph())

                simple_graph = self.convert_to_simple(multi_graph)

                if conf.a_louvain:
                    partition = nxlouvain.best_partition(multi_graph)
                    res_louvain[dt.strftime("%Y-%m-%d")] = partition

                if conf.a_betweenness_centrality:
                    bc = nx.betweenness_centrality(multi_graph, normalized=True)
                    res_betweenness_centrality[dt.strftime("%Y-%m-%d")] = bc

                if conf.a_degree_centrality:
                    dc = nx.degree_centrality(multi_graph)
                    res_degree_centrality[dt.strftime("%Y-%m-%d")] = dc

                if conf.a_modularity:
                    mod = nxlouvain.modularity(partition, multi_graph)
                    res_modularity[dt.strftime("%Y-%m-%d")] = mod

                if conf.a_eigenvector_centrality:
                    pass
                    # TODO: eigenvector centrality calculation fails occasionally
                    # reason may be that nx.eigenvector_centrality() can't handle star graphs
                    # https://stackoverflow.com/questions/43208737/using-networkx-to-calculate-eigenvector-centrality
                    # ec = nx.eigenvector_centrality(simple_graph)
                    # res_eigenvector_centrality[dt.strftime("%Y-%m-%d")] = ec

            if conf.output_verbose:
                print("current: {0} - time: {0:.2f}s".format(dt.date(), time.time() - lap_time))

        print("{0:.2f}s".format(time.time()-time_start))
        print()

        self._modularity = res_modularity
        self._degree_centrality = res_degree_centrality
        self._betweenness_centrality = res_betweenness_centrality
        self._degree_centrality = res_degree_centrality
        self._eigenvector_centrality = res_eigenvector_centrality
        self._partition = res_louvain

    def _export_measures(self):
        if conf.a_betweenness_centrality:
            pd.DataFrame.from_dict(self._betweenness_centrality)\
                .to_csv(conf.get_nx_path(self._owner, self._repo, "bc"))

        if conf.a_degree_centrality:
            pd.DataFrame.from_dict(self._degree_centrality)\
                .to_csv(conf.get_nx_path(self._owner, self._repo, "dc"))

        if conf.a_eigenvector_centrality:
            pd.DataFrame.from_dict(self._eigenvector_centrality) \
                .to_csv(conf.get_nx_path(self._owner, self._repo, "ec"))

        if conf.a_modularity:
            i = "mod"
            df = pd.DataFrame.from_dict(self._modularity, orient="index")
            df.to_csv(conf.get_nx_path(self._owner, self._repo, i))
            if conf.a_gen_charts:
                pass
                # df.plot(title="Modularity ({0}/{1})".format(self._owner, self._repo),
                #         legend=False)
                #  plt.savefig(conf.get_plot_path(self._owner, self._repo, i))

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

    @staticmethod
    def convert_to_simple(multi_graph):
        simple_graph = nx.Graph()
        for u, v, data in multi_graph.edges(data=True):
            w = data['weight'] if 'weight' in data else 1.0

            if simple_graph.has_edge(u, v):
                simple_graph[u][v]['weight'] += w
            else:
                simple_graph.add_edge(u, v, weight=w)

        return simple_graph

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

        print("exporting data in graphJSON format for {0}/{1}".format(self._owner, self._repo))

        groups = self.convert_keys_to_string(self._partition)
        d_centrality = self.convert_keys_to_string(self._degree_centrality)
        b_centrality = self.convert_keys_to_string(self._betweenness_centrality)
        modularity = self.convert_keys_to_string(self._modularity)

        nodes, links, info = self._controller.get_viz_data(self._owner, self._repo)

        data = {"info": info,
                "nodes": nodes,
                "links": links,
                "groups": groups,
                "d_centrality": d_centrality,
                "b_centrality": b_centrality,
                "modularity": modularity}

        with open(conf.get_viz_data_path(self._owner, self._repo), "w") as fp:
            json.dump(data, fp, indent="\t")


if __name__ == '__main__':
    analyze_repos()
