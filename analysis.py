import neocontroller
import pandas as pd
import networkx as nx
import community as nxlouvain
from dateutil import rrule
from datetime import datetime
import conf
import time


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
        self._partition = None
        self._degree_centrality = None
        self._betweenness_centrality = None

    def run(self):
        # self.partition = self.louvain_networkx()
        self._partition, self._degree_centrality, self._betweenness_centrality = self._individual_measures()

    def get_groups(self):
        return self._partition

    def get_betweenness_centrality(self):
        return self._betweenness_centrality

    def get_degree_centrality(self):
        return self._degree_centrality

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

        time_start = time.time()
        for dt in rrule.rrule(rrule.WEEKLY, dtstart=self._startdt, until=self._enddt):
            lap_time = time.time()

            links = self._controller.get_subgraph(self._owner, self._repo, dt)

            if not links.empty:
                nxgraph = nx.from_pandas_dataframe(links, source="source", target="target", create_using=nx.MultiGraph())

                if conf.a_louvain:
                    partition = nxlouvain.best_partition(nxgraph)
                    # partition = self.convert_keys_to_string(partition)
                    res_louvain[dt.strftime("%Y-%m-%d")] = partition

                if conf.a_betweenness_centrality:
                    bc = nx.betweenness_centrality(nxgraph, normalized=True)
                    res_betweenness_centrality[dt.strftime("%Y-%m-%d")] = bc

                if conf.a_degree_centrality:
                    dc = nx.degree_centrality(nxgraph)
                    res_degree_centrality[dt.strftime("%Y-%m-%d")] = dc

            if conf.output_verbose:
                print("current: {0} - time: {0:.2f}s".format(dt.date(), time.time() - lap_time))

        print("{0:.2f}s".format(time.time()-time_start))
        print()

        return res_louvain, res_degree_centrality, res_betweenness_centrality

    def _export_measures(self):
        pass


if __name__ == '__main__':
    analyze_repos()
    # analyze_owners()