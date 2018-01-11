import pandas as pd
from project import Project
from neocontroller import Neo4jController
import time
import collectors
import conf
import json
# import cProfile


def main():
    neo_controller = Neo4jController()

    if conf.neo4j_clear_on_startup:
        print("clearing neo4j database")
        print()
        neo_controller.clear_db()

    if conf.run_analysis:
        run_analysis()

    # TODO: doesn't make sense here anymore
    # if conf.neo4j_run_louvain:
    #     neo_controller.run_louvain()

    # if conf.neo4j_export_json:
    #    neo_controller.export_graphjson()

    collectors.analyze_invalid_refs()

    collectors.analyze_position_nan()

    print("------------------------------------------")
    print("Total process time elapsed:        {0:.2f}s".format(time.process_time()))
    print("------------------------------------------")


def run_analysis():
    import_repos = pd.read_csv("Input/owners.csv", sep=',', header=0)
    owners = import_repos["owners"].unique()

    for owner in owners:
        repos = import_repos[import_repos["owners"] == owner]
        repos = repos["repo_names"]
        split_projects(owner, repos)


def split_projects(owner, repos):
    pullreq_data, issue_data, commit_data = import_owner_data(owner)

    for repo in repos:
        proc_time_start = time.process_time()
        print("---------------------------------")
        print("Starting analysis on  >>> {0}/{1}".format(owner, repo))

        project_pullreq_data = pullreq_data[pullreq_data["repo"] == repo]
        project_issue_data = issue_data[issue_data["repo"] == repo]

        Project(project_pullreq_data, project_issue_data, owner, repo).run()

        # TODO: implement support for commit data

        print("time required:                {0:.2f}s".format(time.process_time() - proc_time_start))
        print()
        print("---------------------------------")
        print()

def import_owner_data(owner):
    with open(conf.get_import_path(owner)) as json_data:
        d = json.load(json_data)

    pullreq_data = pd.DataFrame(d["pc"])
    issue_data = pd.DataFrame(d["ic"])
    commit_data = pd.DataFrame(d["cc"])

    pullreq_data, issue_data, commit_data = clean_data(pullreq_data, issue_data, commit_data)

    print("Imported data for >>> " + owner)

    return pullreq_data, issue_data, commit_data


def clean_data(pc, ic, cc):
    lst = [pc, ic, cc]

    # TODO: find a more elegant solution to handle empty input lists.

    for i, e in enumerate(lst):
        if e.empty:
            # create dummy df
            lst[i] = pd.DataFrame(columns=["user", "repo", "owner", "position", "thread_id", "body"])

        else:
            e = rename_cols(e)
            e = extract_user(e)
            e = infer_datetime(e)
            e = date_filter(e)
            lst[i] = e

        lst[0] = position_na_filter(lst[0])
        lst[2] = position_na_filter(lst[2])

    return lst[0], lst[1], lst[2]


def extract_user(data):
    try:
        if not data["user"].empty:
            if type(data["user"].iloc[0]) is dict:
                for index, row in data.iterrows():
                    data.at[index, "user"] = row["user"].get('login')

            data["user"] = data["user"].str.lower()
    except KeyError:
        print(data)
        raise KeyError
    return data


def position_na_filter(data):
    # nan-filter for positions
    data_nan = data[pd.isna(data["position"])]
    collectors.add_position_nan(data_nan.to_dict('records'))

    data = data[pd.notna(data["position"])]
    return data


def infer_datetime(data):
    data["created_at"] = pd.to_datetime(data["created_at"])
    return data


def date_filter(data):
    data = data[conf.minDate <= data["created_at"]]
    data = data[data["created_at"] <= conf.maxDate]
    return data


def rename_cols(data):

    column_names = data.columns
    if "pullreq_id" in column_names:
        data = data.rename(index=str, columns={"pullreq_id": "thread_id"})
    elif "issue_id" in column_names:
        data = data.rename(index=str, columns={"issue_id": "thread_id"})
    elif "commit_id" in column_names:
        data = data.rename(index=str, columns={"commit_id": "thread_id"})

    return data

if __name__ == '__main__':
    # cProfile.run("main()", sort="cumtime")
    main()
