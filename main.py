"""core file to the network construction process.
To start network construction, run main().
To configure the network construction process, set parameters in conf module"""

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
    neo_controller.clear_db()

    _construct_network()

    neo_controller.export_graphjson()

    collectors.analyze_invalid_refs()
    collectors.analyze_position_nan()

    print("------------------------------------------")
    print("Total process time elapsed:        {0:.2f}s".format(time.process_time()))
    print("------------------------------------------")


def _construct_network():
    """Reads the owner/repository combinations from the file Input/owners.csv. For each owner/repository
    which was filled in there, the network construction process is being started."""

    if not conf.construct_network:
        return

    import_repos = pd.read_csv("Input/owners.csv", sep=',', header=0)
    owners = import_repos["owners"].unique()

    for owner in owners:
        repos = import_repos[import_repos["owners"] == owner]
        repos = repos["repo_names"]
        _split_projects(owner, repos)


def _split_projects(owner, repos):
    """Creates a new Project-object for each owner/repo combination.
    Starts the analysis process on each Project"""

    pullreq_data, issue_data, commit_data = _import_comment_data(owner)

    for repo in repos:
        proc_time_start = time.process_time()
        print("---------------------------------")
        print("Starting analysis on  >>> {0}/{1}".format(owner, repo))

        project_pullreq_data = pullreq_data[pullreq_data["repo"] == repo]
        project_issue_data = issue_data[issue_data["repo"] == repo]

        Project(project_pullreq_data, project_issue_data, commit_data, owner, repo).run()

        print("time required:                {0:.2f}s".format(time.process_time() - proc_time_start))
        print()
        print("---------------------------------")
        print()


def _import_comment_data(owner):
    """loads comment data owner wise from json dumps"""

    with open(conf.get_data_path(owner)) as json_data:
        d = json.load(json_data)

    pullreq_data = pd.DataFrame(d["pc"])
    issue_data = pd.DataFrame(d["ic"])
    commit_data = pd.DataFrame(d["cc"])

    pullreq_data, issue_data, commit_data = _clean_comment_data(pullreq_data, issue_data, commit_data)

    print("Imported data for >>> " + owner)

    return pullreq_data, issue_data, commit_data


def _clean_comment_data(pc, ic, cc):
    """infers data cleaning on the raw comment data json input"""

    lst = [pc, ic, cc]

    # TODO: find a more elegant solution to handle empty input lists.

    for i, e in enumerate(lst):
        if e.empty:
            # create dummy df
            lst[i] = pd.DataFrame(columns=["user", "repo", "owner", "position", "thread_id", "body"])

        else:
            e = _rename_cols(e)
            e = _extract_user(e)
            e = _infer_datetime(e)
            e = _date_filter(e)
            lst[i] = e

        lst[0] = _position_na_filter(lst[0])
        lst[2] = _position_na_filter(lst[2])

    return lst[0], lst[1], lst[2]


def _extract_user(data):
    """data from mongoDB can be nested in two levels. this method gets the username if it is nested
    like user:{login:'name'} """
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


def _position_na_filter(data):
    """takes out commit or pullreq comments that do not have a valid position such that they can't be
    split into comment threads later"""

    # nan-filter for positions
    data_nan = data[pd.isna(data["position"])]
    collectors.add_position_nan(data_nan.to_dict('records'))

    data = data[pd.notna(data["position"])]
    return data


def _infer_datetime(data):
    data["created_at"] = pd.to_datetime(data["created_at"])
    return data


def _date_filter(data):
    """filters entries that do not lie within the date range defined in the config file"""
    data = data[conf.minDate <= data["created_at"]]
    data = data[data["created_at"] <= conf.maxDate]
    return data


def _rename_cols(data):
    # TODO: find another name for thread_id
    # the name thread_id is incorrect as for cc and pc threads have to be split further by position. Fi
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
