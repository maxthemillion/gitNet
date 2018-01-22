import neocontroller
import pandas as pd
import networkx as nx
import community as nxlouvain
from dateutil import rrule
from datetime import datetime
import conf
import time
import numpy as np
import matplotlib.pyplot as plt


def analyze_repos():
    import_repos = pd.read_csv("Input/owners.csv", sep=',', header=0)
    owners = import_repos["owners"].unique()

    for owner in owners:
        repos = import_repos[import_repos["owners"] == owner]
        repos = repos["repo_names"]
        for repo in repos:
            analyzer = Analyzer(owner, repo)
            analyzer.run()


def analyze_owners():
    # read in owners list
    # for each owner, run analysis
    pass


class Analyzer:
    def __init__(self, owner, repo):
        self._owner = owner
        self._repo = repo

        self._controller = neocontroller.Neo4jController()
        self._startdt, self._enddt = self._controller.get_timeframe(self._owner, self._repo)

        self._degree_centrality = None
        self._betweenness_centrality = None
        self._eigenvector_centrality = None

        self._partition = None

        self._modularity = None

        self._call_as_class = not __name__ == '__main__'

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

            links = self._controller.get_subgraph(self._owner, self._repo, dt)

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

            links = self._controller.get_subgraph(self._owner, self._repo, dt)

            if not links.empty:
                multi_graph = nx.from_pandas_dataframe(links, source="source", target="target", create_using=nx.MultiGraph())
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
                    ec = nx.eigenvector_centrality(simple_graph)
                    res_eigenvector_centrality[dt.strftime("%Y-%m-%d")] = ec

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
                df.plot(title="Modularity ({0}/{1})".format(self._owner, self._repo),
                        legend=False)
                plt.savefig(conf.get_plot_path(self._owner, self._repo, i))


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


if __name__ == '__main__':
    analyze_repos()
    # analyze_owners()