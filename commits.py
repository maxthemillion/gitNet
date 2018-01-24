import pandas as pd
from neocontroller import Neo4jController
import warnings
import pandas as pd


def _complete_import():
    import_repos = pd.read_csv("Input/owners.csv", sep=',', header=0)
    owners = import_repos["owners"].unique()

    for o in owners:
        repos = import_repos[import_repos["owners"] == o]
        repos = repos["repo_names"]

        for r in repos:
            import_repo(o, r)


def import_repo(o, r):
    print("Importing commits for {0}/{1}".format(o,r))
    import_path = "Input/com/{0}_{1}_commits.csv".format(o, r)

    try:
        d = pd.read_csv(import_path, sep=',', header=0)

        d["created_at"] = pd.to_datetime(d["created_at"])
        d["login"] = d["login"].str.lower()

        controller = Neo4jController()
        controller.import_commits(d, o, r)

    except FileNotFoundError:
        warnings.warn("file not found: " + import_path)

    print()

if __name__ == '__main__':
    _complete_import()
