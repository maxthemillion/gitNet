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
        self.owner = owner
        self.repo = repo

        self.controller = neocontroller.Neo4jController()
        self.startdt, self.enddt = self.controller.get_timeframe(self.owner, self.repo)
        self.partition = None
        self.degree_centrality = None
        self.betweenness_centrality = None

    def run(self):
        # self.partition = self.louvain_networkx()
        self.partition, self.degree_centrality, self.betweenness_centrality = self.run_all()

    def louvain_networkx(self):

        print("Running NX Louvain algorithm for {0}/{1} and timeframe length {2}".format(self.owner, self.repo,
                                                                                         conf.a_length_timeframe))
        res = {}
        for dt in rrule.rrule(rrule.WEEKLY, dtstart=self.startdt, until=self.enddt):
            time_start = time.time()

            links = self.controller.get_subgraph(self.owner, self.repo, dt)

            if not links.empty:
                nxgraph = nx.from_pandas_dataframe(links, source="source", target="target", create_using=nx.MultiGraph())

                partition = nxlouvain.best_partition(nxgraph)

                # partition = self.convert_keys_to_string(partition)

                res[dt.strftime("%Y-%m-%d")] = partition

            print("current: {0} - time: {1}".format(dt.date(), time.time() - time_start))

        return res


    def run_all(self):

        print("Running NX analysis for {0}/{1} and timeframe length {2}".format(self.owner, self.repo,
                                                                                        conf.a_length_timeframe))
        res_louvain = {}
        res_degree_centrality = {}
        res_betweenness_centrality = {}
        for dt in rrule.rrule(rrule.WEEKLY, dtstart=self.startdt, until=self.enddt):
            time_start = time.time()

            links = self.controller.get_subgraph(self.owner, self.repo, dt)

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

            print("current: {0} - time: {1}".format(dt.date(), time.time() - time_start))

        return res_louvain, res_degree_centrality, res_betweenness_centrality


if __name__ == '__main__':
    # cProfile.run("main()", sort="cumtime")
    analyze_repos()
    # analyze_owners()