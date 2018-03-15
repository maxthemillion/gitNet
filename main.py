"""
MODULE: main

core script to the network construction process. To start network construction, run main().
To configure the network construction process, set parameters in conf module
"""

import time
import pandas as pd
import conf as conf
from classes.project import Project
import cProfile
import logging
import os


def main():
    """
    Calls the network construction process.

    :return:    --
    """
    time_start = time.time()

    _construct_network()

    print("------------------------------------------")
    print("Total process time elapsed:        {0:.2f}s".format(time.process_time()))
    print("Total time elapsed:                {0:.2f}s".format(time.time() - time_start))
    print("------------------------------------------")


def _construct_network():
    """
    Reads the owner/repository combinations from the file Import_Network/owners.csv. For each owner/repository
    which was filled in there, the network construction process is being started.

    file repos.csv provides owner-repository relations in a two-column format:
    header: owner_login, repo_id

    :return:    --
    """

    if not conf.construct_network:
        return

    import_repos = pd.read_csv("Data/Import_Network/repos.csv", sep=',', header=0)
    owners = import_repos["owner_login"].unique()

    counter = 0
    num_owners = len(owners)
    for owner in owners:

        repos = import_repos[import_repos["owner_login"] == owner]
        repos = repos["repo_id"]
        _split_projects(owner, repos)

        counter += 1
        print("progress: {0}/{1}".format(counter, num_owners))
        logging.info("processed: {0} ({1}/{2})".format(owner, counter, num_owners))

        with open("processed_owners.csv", 'w') as po:
            po.write(owner)


def _split_projects(owner: str, repos: pd.Series):
    """
    Creates a new Project-object for each owner/repo combination.
    Starts the analysis process on each Project

    :param owner:       owner name
    :param repos:       pd.Series containing repository names
    :return:            --
    """

    pullreq_data, issue_data, commit_data = _import_comment_data(owner)

    for repo in repos:
        proc_time_start = time.process_time()

        if conf.output_verbose:
            print(">>> analyzing {0}/{1}".format(owner, repo))
        else:
            print("analyzing {0}/{1}".format(owner, repo))

        project_pullreq_data = pullreq_data[pullreq_data["repo_id"] == repo]
        project_issue_data = issue_data[issue_data["repo_id"] == repo]

        Project(project_pullreq_data, project_issue_data, commit_data, owner, repo).run()

        if conf.output_verbose:
            print("time required:                {0:.2f}s".format(time.process_time() - proc_time_start))
            print()
            print("---------------------------------")
            print()
        else:
            print("{0:.2f}s".format(time.process_time() - proc_time_start))
            print()


def _import_comment_data(owner: str) -> (pd.DataFrame, pd.DataFrame, pd.DataFrame):
    """
    loads comment data from files.

    :param owner:       owner name
    :return:            tuple of pullrequest-, issue- and comment data
    """
    try:
        pc_data = pd.read_csv(conf.get_pc_data_path(owner))
    except FileNotFoundError:
        pc_data = None
        print('file not found')

    try:
        ic_data = pd.read_csv(conf.get_ic_data_path(owner))
    except FileNotFoundError:
        ic_data = None
        print('file not found')

    try:
        cc_data = pd.read_csv(conf.get_cc_data_path(owner))
    except FileNotFoundError:
        cc_data = None
        print('file not found')

    p_data, i_data, c_data = _clean_comment_data(pc_data, ic_data, cc_data)

    print("Imported data for >>> " + owner)

    return p_data, i_data, c_data


def _clean_comment_data(pc, ic, cc):
    """
    infers data cleaning on the raw comment input

    :param pc:  pull request comments
    :param ic:  issue comments
    :param cc:  commit comments
    :return:
    """

    dummy_df = pd.DataFrame(columns=["owner_name",
                                     "repo_id",
                                     "actor_id",
                                     "actor_login",
                                     "comment_body",
                                     "comment_id",
                                     "comment_position",
                                     "thread_id"])

    if pc is None:
        pc = dummy_df
    else:
        pc = _position_na_filter(pc)

    if ic is None:
        ic = dummy_df

    if cc is None:
        cc = dummy_df
    else:
        cc = _position_na_filter(cc)

    pc = _rename_cols(pc)
    ic = _rename_cols(ic)
    cc = _rename_cols(cc)

    return pc, ic, cc


def _position_na_filter(data):
    """
    sets -1 on comments without valid position. These comments have been made to the commit in general and not to a
    specific line in the diff.

    :param data:  data frame containing comment data
    :return:      data frame where na values have been replaced in the comment_position column
    """

    data["comment_position"] = data["comment_position"].fillna(value=-1)

    return data


def _rename_cols(data):
    """

    :param data:
    :return:
    """

    # TODO: find another name for thread_id
    # the name thread_id is incorrect as for cc and pc threads have to be split further by position. Fi
    column_names = data.columns
    if "pull_request_id" in column_names:
        data = data.rename(index=str, columns={"pull_request_id": "thread_id"})
    elif "issue_id" in column_names:
        data = data.rename(index=str, columns={"issue_id": "thread_id"})
    elif "commit_id" in column_names:
        data = data.rename(index=str, columns={"commit_id": "thread_id"})

    return data


def clean_up():
    # rename references file
    time_str = time.strftime("%y-%m-%d %H:%M:%S")
    os.rename("Data/Relations/relations.csv",
              "Data/Relations/" + time_str + "_relations.csv")

    os.rename("processed_owners.csv",
              time_str + "_processed_owners.csv")


def set_up():
    relations_file = "Data/Relations/relations.csv"
    file_exists = os.path.isfile(relations_file)
    if file_exists:
        os.remove(relations_file)
        logging.info("removed relations.csv")

    processed_owners_file = "processed_owners.csv"
    file_exists = os.path.isfile(processed_owners_file)
    if file_exists:
        os.remove(processed_owners_file)
        logging.info("removed processed_owners.csv")


if __name__ == '__main__':

    logging.basicConfig(filename='NC.log', level=logging.INFO, format='%(levelname)s: %(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')

    logging.info("Initialized")
    logging.warning("starting process now")

    set_up()

    logging.info("Started reference detection")

    try:
        # cProfile.run("main()", sort="cumtime")
        main()
    except Exception as e:
        logging.exception(str(e))

    logging.info("Finished reference detection")

    clean_up()

    logging.info("Completed")
