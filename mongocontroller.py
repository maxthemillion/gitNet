"""This script collects all data for further analysis. It is intended to be run once.
All data is collected owner wise and stored to json dumps.

To run this script, establish an ssh connection using
$ ssh -L 27017:dutihr.st.ewi.tudelft.nl:27017 ghtorrent@dutihr.st.ewi.tudelft.nl
and then in a different window
$ mongo -u ghtorrentro -p ghtorrentro github"""


import pymongo
import pandas as pd
import json
import time
import conf


def main():
    _collect_comment_data()
    _collect_user_data()
    _collect_project_data()


def _collect_project_data():
    """Collects all required project data and dumps it into json files"""
    pass


def _collect_user_data():
    """Collects all required user data and dumps it into json files"""
    pass


def _collect_commit_data():
    """Collects all required commit data and dumps it into json files"""
    pass


def _collect_comment_data():
    """Collects all comment data and dumps it to json files owner wise.
    It gets issue-, pullreq- and commit-comments from GHTorrent MongoDB"""

    mongo_c = pymongo.MongoClient()
    db = mongo_c.github

    coll_issue_c = db.issue_comments
    coll_pullreq_c = db.pull_request_comments
    coll_commit_c = db.commit_comments

    owners_list = pd.read_csv("Input/owners.csv", sep=',', header=0)
    owners = owners_list["owners"].unique()

    for owner in owners:
        time_start = time.time()

        repos = owners_list[owners_list["owners"] == owner]
        repos = repos["repo_names"]

        issue_comments = _yield_issue_comments(coll_issue_c, repos, owner)
        pullreq_comments = _yield_pullreq_comments(coll_pullreq_c, repos, owner)
        commit_comments = _yield_commit_comments(coll_commit_c, _yield_commit_shas(owner))

        d = {"ic": issue_comments,
             "pc": pullreq_comments,
             "cc": commit_comments}

        with open(conf.get_data_path(owner), "w") as fp:
            json.dump(d, fp, indent="\t")

        print("total time required:             {0:.2f}s".format(time.time() - time_start))
        print("issue comments retrieved:        {0}".format(len(issue_comments)))
        print("pullreq comments retrieved:      {0}".format(len(pullreq_comments)))
        print("commit comments retrieved:       {0}".format(len(commit_comments)))
        print()


def _check_consistency(d):
    # TODO: do some consistency checks
    return d


def _yield_issue_comments(c, repos, owner):
    time_start = time.time()
    print("Reading in >>> {0}".format(owner))

    result = []
    for repo in repos:
        print("\trepo >>> {0}".format(repo))

        try:
            lst = list(c.find({"repo": repo, "owner": owner},
                              {"_id": 0,
                               "id": 1,
                               "user.login": 1,
                               "owner": 1,
                               "repo": 1,
                               "body": 1,
                               "created_at": 1,
                               "issue_id": 1
                               }))

            print("issue comments retrieved:           {0:.2f}s".format(time.time() - time_start))

        except pymongo.errors.OperationFailure:
            lst = []

            print("Operation interrupted. Write to log file")
            print("returning empty list")

            with open('log.txt', 'a') as logfile:
                logfile.write('failure when importing issue comments for >>> {0}'.format(owner))

        result.extend(lst)

    return result


def _yield_pullreq_comments(c, repos, owner):
    time_start = time.time()
    print("Reading in >>> {0}".format(owner))

    result = []
    for repo in repos:
        print("\trepo >>> {0}".format(repo))

        try:
            lst = list(c.find({"repo": repo, "owner": owner},
                              {"_id": 0,
                               "id": 1,
                               "user.login": 1,
                               "owner": 1,
                               "repo": 1,
                               "body": 1,
                               "created_at": 1,
                               "pullreq_id": 1,
                               "position": 1
                               }))

            print("pc retrieved:        {0:.2f}s".format(time.time() - time_start))

        except pymongo.errors.OperationFailure:
            lst = []

            print("Operation interrupted. Write to log file")
            print("returning empty list")

            with open('log.txt', 'a') as logfile:
                logfile.write('import failure (pc) >>> {0}/{1}'.format(owner, repo))

        result.extend(lst)

    return result


def _yield_commit_comments(c, commit_shas):
    time_start = time.time()
    print("Reading in commit_comments")

    try:
        lst = list(c.find({"commit_id": {"$in": commit_shas}},
                          {"_id": 0,
                           "id": 1,
                           "user.login": 1,
                           "body": 1,
                           "created_at": 1,
                           "commit_id": 1,
                           "repo": 1,
                           "position": 1
                           }))
        print("cc retrieved:        {0:.2f}s".format(time.time() - time_start))

    except pymongo.errors.OperationFailure:
        print("Operation interrupted. Write to log file")
        print("returning empty list")

        with open('log.txt', 'a') as logfile:
            logfile.write('import failure (cc)')

        lst = []

    return lst


def _yield_commit_shas(owner, repo):
    """reads in the commit shas provided in Input/cc/. the shas identify commits to which commit comments were made"""
    pd.read_csv()
    commit_sha = pd.read_csv("Input/cc/commit_shas_{0}_{1}.csv"
                              .format(owner.lower(), repo.lower()), header=0)

    return commit_sha.to_list()


if __name__ == '__main__':
    # cProfile.run("main()", sort="cumtime")
    main()

    print("------------------------------------------")
    print("Total process time elapsed:      {0:.2f}s".format(time.process_time()))
    print("------------------------------------------")
