import pandas as pd
from neocontroller import Neo4jController

import pandas as pd


def import_commits():
    #TODO: enlarge data import to all projects in owners.csv

    import_repos = pd.read_csv("Input/owners.csv", sep=',', header=0)
    owners = import_repos["owners"].unique()

    d = pd.read_csv("Input/com/Homebrew_brew_commits.csv", sep=',', header=0)
    d["created_at"] = pd.to_datetime(d["created_at"])
    d["login"] = d["login"].str.lower()

    controller = Neo4jController()
    controller.import_commits(d, "Homebrew", "brew")
